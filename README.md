# QuickCart: Mini Distributed Application (Distributed Systems Mini Project)

Contents
- docs/QuickCart_Report.pdf : design and analysis report (scalability, communication,
  failure handling, resource sharing)
- src/quickcart.py : in-process simulation of the architecture (services + broker, stdlib only)
- src/demo.py : three experiments that check the key invariants

Run (Python 3.8+, no dependencies):
    cd src && python demo.py

What the demo shows
1. Concurrency: 300 orders compete for 100 units -> exactly 100 confirmed, stock never negative.
2. Message duplication: idempotent consumers ignore duplicates -> no double reservation/charge.
3. Failing payment provider: retries with backoff + circuit breaker; failed payments trigger the
   saga compensation (stock released), so confirmed orders == charges == reserved stock.

Mapping to the report: Sec 6 (idempotency, at-least-once), Sec 7 (retry, circuit breaker, saga),
Sec 8 (atomic conditional update). This is a teaching simulation; a real deployment would use
Kafka/RabbitMQ, PostgreSQL, Redis and Kubernetes as described in Section 3.
