import collections, random, threading
from quickcart import QuickCart

random.seed(7)

def run(title, stock, orders, payment_fail=0.0, dup=0.0):
    app = QuickCart({"P1": stock}, payment_fail, dup)
    threads = [threading.Thread(target=app.place_order, args=("P1",)) for _ in range(orders)]
    [t.start() for t in threads]; [t.join() for t in threads]
    app.wait(orders)
    res = collections.Counter(app.ord.done.values())
    avail, reserved = app.inv.db.execute("SELECT available, reserved FROM stock").fetchone()
    app.stop()
    print(f"\n== {title} ==")
    print("orders:", dict(res)); print(f"stock available={avail} reserved={reserved} charges={app.pay.charges}")
    assert avail >= 0, "OVERSOLD!"
    assert res.get("CONFIRMED", 0) == reserved == app.pay.charges, "inconsistent!"
    print("invariants OK: no overselling, one charge per confirmed order")

run("1. Contention: 300 orders for 100 units", 100, 300)
run("2. Duplicated messages (30%)", 100, 200, dup=0.3)
run("3. Flaky payment provider (40% failures) + saga compensation", 100, 150, payment_fail=0.4)
