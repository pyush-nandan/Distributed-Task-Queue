# Distributed Task Queue

A robust, distributed task queue implemented in Java and backed by PostgreSQL.

Every production backend — whether dispatching orders, processing payments, or running CI jobs — relies on reliable background task processing. This project is built to demonstrate how these systems work under the hood, explicitly handling crashes, stuck jobs, partial failures, and concurrent worker coordination. 

By leveraging Java's concurrency model (`ThreadPoolExecutor`, `ScheduledExecutorService`, graceful shutdown hooks) and PostgreSQL's advanced row-level locking mechanisms (`FOR UPDATE SKIP LOCKED`), this queue ensures that tasks are processed reliably and without race conditions, even in distributed, multi-worker environments.

---

## Architecture Decisions & Tradeoffs

### PostgreSQL over Redis
Redis is in-memory; a crash means lost tasks. PostgreSQL's disk-backed WAL ensures durability. It also provides true ACID transactions for atomic task claiming, whereas Redis lacks native row-level locking.

### Polling over Push
Push systems (RabbitMQ/Kafka) require separate broker infrastructure. Polling PostgreSQL keeps our architecture simple by reusing the existing database. **Tradeoff**: Increased latency (tasks wait for the next poll cycle).

### At-Least-Once over Exactly-Once
Exactly-once delivery requires complex two-phase commits spanning the DB and external APIs, which is often impossible. At-least-once is simpler and more reliable. **Tradeoff**: Task logic must be strictly idempotent.

### `SKIP LOCKED` over Optimistic Locking
Under high load, optimistic locking causes severe worker contention and wasted retries. PostgreSQL's `FOR UPDATE SKIP LOCKED` allows workers to efficiently bypass locked rows and grab the next available task, scaling linearly without database churn.

### Scaling to 100x
Eventually, the single PostgreSQL instance (specifically its lock manager) will bottleneck. To scale further, we would shard the tasks table across multiple databases or migrate to an event-streaming platform like Kafka to eliminate row-level locking entirely.