"""QuickCart: mini distributed order-processing simulation (single process, stdlib only).

Each "service" runs in its own thread with its own SQLite database and talks to the
others only through a message broker (queues), as in the report. Demonstrates:
  * at-least-once delivery + idempotent consumers
  * atomic conditional stock reservation (no overselling)
  * saga with compensation (payment failure releases stock)
  * retry with exponential backoff + jitter, circuit breaker
  * duplicate message injection
"""
import queue, random, sqlite3, threading, time, uuid


class Broker:
    """Topic -> queue. Can duplicate messages to mimic at-least-once delivery."""
    def __init__(self, duplicate_rate=0.0):
        self.topics, self.lock, self.dup = {}, threading.Lock(), duplicate_rate
    def q(self, topic):
        with self.lock:
            return self.topics.setdefault(topic, queue.Queue())
    def publish(self, topic, msg):
        self.q(topic).put(msg)
        if random.random() < self.dup:          # network duplicates a message
            self.q(topic).put(dict(msg))


class Service(threading.Thread):
    def __init__(self, name, broker, topic, handler):
        super().__init__(daemon=True)
        self.name, self.broker, self.topic, self.handler = name, broker, topic, handler
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.execute("CREATE TABLE processed(key TEXT PRIMARY KEY)")
        self.running = True
    def run(self):
        q = self.broker.q(self.topic)
        while self.running:
            try: msg = q.get(timeout=0.1)
            except queue.Empty: continue
            key = f"{msg['type']}:{msg['order_id']}"
            try: self.db.execute("INSERT INTO processed VALUES(?)", (key,))
            except sqlite3.IntegrityError: continue      # duplicate -> ignore (idempotency)
            self.handler(self, msg)
            self.db.commit()


# ---------------- Inventory ----------------
def inventory_setup(svc, stock):
    svc.db.execute("CREATE TABLE stock(pid TEXT PRIMARY KEY, available INT, reserved INT)")
    svc.db.execute("CREATE TABLE reservation(order_id TEXT PRIMARY KEY, pid TEXT, qty INT)")
    for pid, n in stock.items(): svc.db.execute("INSERT INTO stock VALUES(?,?,0)", (pid, n))
    svc.db.commit()

def inventory_handler(svc, m):
    if m["type"] == "OrderCreated":
        cur = svc.db.execute(   # atomic check-and-decrement: one statement decides
            "UPDATE stock SET available=available-?, reserved=reserved+? "
            "WHERE pid=? AND available>=?", (m["qty"], m["qty"], m["pid"], m["qty"]))
        if cur.rowcount == 1:
            svc.db.execute("INSERT INTO reservation VALUES(?,?,?)", (m["order_id"], m["pid"], m["qty"]))
            svc.broker.publish("payment", {**m, "type": "StockReserved"})
        else:
            svc.broker.publish("order", {**m, "type": "StockRejected"})
    elif m["type"] == "ReleaseStock":     # saga compensation
        r = svc.db.execute("SELECT pid, qty FROM reservation WHERE order_id=?", (m["order_id"],)).fetchone()
        if r:
            svc.db.execute("UPDATE stock SET available=available+?, reserved=reserved-? WHERE pid=?", (r[1], r[1], r[0]))
            svc.db.execute("DELETE FROM reservation WHERE order_id=?", (m["order_id"],))


# ---------------- Payment ----------------
class CircuitBreaker:
    def __init__(self, threshold=5, cooldown=0.2):
        self.fails, self.threshold, self.cooldown, self.opened = 0, threshold, cooldown, 0
    def call(self, fn):
        if self.fails >= self.threshold and time.time() - self.opened < self.cooldown:
            raise RuntimeError("circuit open")
        try:
            r = fn(); self.fails = 0; return r
        except Exception:
            self.fails += 1; self.opened = time.time(); raise

def retry(fn, attempts=3, base=0.01, cap=0.1):
    for i in range(attempts):
        try: return fn()
        except Exception:
            if i == attempts - 1: raise
            time.sleep(random.uniform(0, min(cap, base * 2 ** i)))   # exponential backoff + full jitter

def payment_setup(svc, fail_rate):
    svc.db.execute("CREATE TABLE payment(order_id TEXT PRIMARY KEY, status TEXT)")
    svc.fail_rate, svc.breaker, svc.charges = fail_rate, CircuitBreaker(), 0

def payment_handler(svc, m):
    def provider():                       # simulated flaky external provider
        if random.random() < svc.fail_rate: raise ConnectionError("provider timeout")
        svc.charges += 1
    try: retry(lambda: svc.breaker.call(provider)); ok = True
    except Exception: ok = False
    svc.db.execute("INSERT INTO payment VALUES(?,?)", (m["order_id"], "PAID" if ok else "FAILED"))
    svc.broker.publish("order", {**m, "type": "PaymentCompleted" if ok else "PaymentFailed"})


# ---------------- Order (saga coordinator) ----------------
def order_setup(svc):
    svc.db.execute("CREATE TABLE orders(order_id TEXT PRIMARY KEY, status TEXT)")
    svc.done = {}

def order_handler(svc, m):
    status = {"PaymentCompleted": "CONFIRMED", "StockRejected": "CANCELLED_NO_STOCK",
              "PaymentFailed": "CANCELLED_PAYMENT"}[m["type"]]
    svc.db.execute("INSERT OR REPLACE INTO orders VALUES(?,?)", (m["order_id"], status))
    if status == "CANCELLED_PAYMENT":     # compensation
        svc.broker.publish("inventory", {**m, "type": "ReleaseStock"})
    svc.done[m["order_id"]] = status


class QuickCart:
    def __init__(self, stock, payment_fail=0.0, duplicate_rate=0.0):
        self.b = Broker(duplicate_rate)
        self.inv = Service("inventory", self.b, "inventory", inventory_handler); inventory_setup(self.inv, stock)
        self.pay = Service("payment", self.b, "payment", payment_handler);       payment_setup(self.pay, payment_fail)
        self.ord = Service("order", self.b, "order", order_handler);             order_setup(self.ord)
        self.services = [self.inv, self.pay, self.ord]
        for s in self.services: s.start()
    def place_order(self, pid, qty=1):
        oid = str(uuid.uuid4())
        self.b.publish("inventory", {"type": "OrderCreated", "order_id": oid, "pid": pid, "qty": qty})
        return oid
    def wait(self, n, timeout=30):
        end = time.time() + timeout
        while len(self.ord.done) < n and time.time() < end: time.sleep(0.05)
        time.sleep(0.3)   # let compensations drain
    def stop(self):
        for s in self.services: s.running = False
