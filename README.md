# flash-sales

A study project for exploring **concurrent systems at scale and async
execution contexts**, using a limited-stock flash sale as the running
scenario: many concurrent buyers racing against a fixed inventory.

- **Backend**: Go, standard library `net/http` for the API, plus
  [`segmentio/kafka-go`](https://github.com/segmentio/kafka-go) for
  the async checkout pipeline described below.
- **Frontend**: Next.js (App Router, TypeScript, Tailwind) — a client
  for visualizing backend behavior: a product list, a buy button, and
  live order-status updates pushed over Server-Sent Events.
- **Messaging**: Kafka (KRaft mode, single broker) backs the async
  handoff between the pieces described below.

## Architecture

### Product catalog

`GET /products` is served by a small DDD-style `Product` aggregate
(`backend/internal/product`) — a constructor that enforces invariants
(non-empty name, non-negative price/stock) — backed by
`PostgresRepository`, which also enforces the stock invariant itself
via atomic `UPDATE` statements (see `DecrementStock`/`ReleaseStock`).
An `InMemoryRepository` still exists as a lightweight test double.

**A known limitation worth being explicit about:** `ReleaseStock` is
bounded by an `initial_stock` column — fixed once at product
creation, never updated afterward — so a release can never push
`stock` above what the product started with (see
`TestReleaseStock_ExceedsInitialStock`). This is **wrong for a real
system**: a product is normally restocked repeatedly over its
lifetime (new inventory arrives, quantities get adjusted), and
`initial_stock` only ever reflects the *first-ever* stock-taking — it
doesn't know about a legitimate restock, so it would eventually start
rejecting perfectly valid releases as the real ceiling drifts upward
while `initial_stock` stays frozen. The correct real-world fix is a
*dynamic* bound instead of a static one: either a `reserved` counter
(incremented by `DecrementStock`, decremented by `ReleaseStock`, so
the check becomes "can't release more than is currently reserved" —
survives any number of restocks), or the `orders`/`order_items`
ledger (now wired into the checkout flow, see below), which would
track the exact quantity tied to each specific reservation rather
than a single product-wide number. `initial_stock` was a deliberate
simplification for this pass, not a design worth carrying into
anything beyond a study project.

### Checkout flow (fire-and-forget, Kafka-backed)

`POST /checkout` (body: `{productId, quantity}`) doesn't wait for a
reservation to be decided — it validates synchronously, then hands
off to an async pipeline of three workers:

![Checkout architecture: POST /checkout hands off to Worker A (StockWorker) via a buffered FIFO channel, which publishes to the checkout.reservations Kafka topic; Worker B (EventConsumer) consumes it, publishes to order.status, and can send a compensating release back to Worker A; Worker C (OrderStatusBroadcaster) consumes order.status and fans it out over SSE to every client connected to GET /events](docs/images/architecture.png)

- **Worker A — StockWorker**: the ONLY goroutine allowed to mutate
  `Product.stock` — serializing every request through one writer is
  what makes the invariant safe with no lock on `Product` itself.
  Decrements stock, or marks the request rejected (out of stock),
  then publishes a `ReservationEvent` to `checkout.reservations`.
- **Worker B — EventConsumer** (Kafka consumer group, manual offset
  commit): waits ~3s (fake latency, simulating e.g. payment
  confirmation). Reserved requests get a random outcome — 80%
  approved, 20% rejected. On rejection, it sends a `ReleaseRequest`
  back to Worker A, which adds the stock back (a compensating
  action — nothing is permanently lost to a rejected order), then
  publishes an `OrderStatusEvent` to `order.status`.
- **Worker C — OrderStatusBroadcaster** (Kafka consumer group): fans
  every `order.status` event out, over Server-Sent Events, to every
  currently-connected browser (`GET /events`).
- **Frontend (EventSource)**: matches incoming events against the
  request id it's waiting on, and updates that product's buy button
  from "Awaiting confirmation..." to an approved/rejected result.

A few things this design deliberately demonstrates:
- **Single-writer instead of locking** — Worker A needs no mutex on
  `Product` because it's the only goroutine that ever touches it.
- **Backpressure at the edge** — the intake channel is bounded; once
  full, new requests get an immediate `503` instead of piling up.
- **At-least-once delivery** — Worker B and Worker C commit their
  Kafka offsets only *after* processing, so a crash mid-processing
  redelivers the message rather than silently dropping it.
- **A visible bottleneck** — Worker B's fake 3s latency caps order
  confirmation throughput to roughly one every 3 seconds, so a burst
  of checkouts visibly queues up in `order.status` even though stock
  reservations (Worker A) happen almost instantly.
- **Broadcast/fan-out** — Worker C's SSE stream is a different async
  pattern than A/B's queue-consumption: every connected client gets
  every event, rather than each message going to exactly one
  consumer.

### Order persistence

`orders`/`order_items` (`backend/internal/order`) are a DDD aggregate
mirroring `Product`'s shape: a validating constructor, private
fields, and `Order.status` is a state machine the aggregate enforces
itself (`pending → reserved → approved`, `pending → rejected`
immediately for out-of-stock, `reserved → rejected` for a
confirmation-stage rejection). `approved`/`rejected` are terminal —
every transition method refuses to run again once an order reaches
one, which is a deliberate down payment on future idempotent-consumer
work (a redelivered Kafka message that tries to re-apply an
already-applied transition gets refused by the domain model itself,
not just by careful consumer code).

No new Kafka consumer was added to write these rows. `Handler`
creates the order (`pending`) synchronously before enqueueing; Worker
A marks it `reserved`/`rejected` right where it already publishes
`ReservationEvent`; Worker B marks it `approved`/`rejected` right
where it already publishes `OrderStatusEvent`. Rows are correlated by
`orders.request_id`, not primary key — the same `RequestID` already
threaded through `Request`/`ReservationEvent`/`OrderStatusEvent`.
Extending the existing publishers instead of adding a third consumer
of the same topics is deliberate: a future transactional outbox needs
the Postgres write and the Kafka publish to happen in the same
component, and this keeps that composable later without a rewrite.

Two simplifications worth being explicit about:
- **Buyer identity is out of scope.** There's no auth. Every order is
  placed under one fixed, seeded "guest" consumer
  (`order.GuestConsumerID`) — the focus here is event-driven
  architecture, not e-commerce.
- **Order-status write failures are best-effort.** If `StockWorker`
  or `EventConsumer` fail to update the order row, they log it but
  still publish the Kafka event regardless — consistent with how a
  `Publish` failure a few lines later is already handled, and exactly
  the gap a future transactional outbox pattern would close.
  `Handler`'s initial order creation, by contrast, fails closed
  (`500`, nothing enqueued) since nothing else has happened yet at
  that point.

### Endpoints

| Endpoint | Method | Notes |
|---|---|---|
| `/health` | GET | liveness check |
| `/products` | GET | list of products (currently one, hardcoded) |
| `/checkout` | POST | `{productId, quantity}` → `202 {requestId}`, or `400`/`404`/`503` |
| `/events` | GET | Server-Sent Events stream of `OrderStatusEvent`s |

## Running locally

`schema.sql` only runs on a fresh Postgres volume (see the comment at
the top of that file) — if you already have a local `postgres_data`
volume from before order persistence was added, run
`docker compose down -v` first so the `orders.request_id` column and
the seeded guest consumer actually get created.

Kafka needs its topics created once before the backend can publish
to them (auto-creation is intentionally off, so partition counts are
explicit):

```sh
docker compose up -d kafka

docker exec flash-sales-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create --topic checkout.reservations \
  --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

docker exec flash-sales-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create --topic order.status \
  --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

Then bring up the rest of the stack:

```sh
docker compose up --build
```

- Backend: http://localhost:8081 (health check at `/health`)
- Frontend: http://localhost:3000
- Kafka broker: `localhost:9092` (for `kafka-topics.sh` from the host)

Kafka's data is stored in a named volume (`kafka_data`), so topics
survive a `docker compose down` — only creating them is a one-time
step, not something needed on every restart.

All three services bind-mount their source directories and hot-reload
on change (the Go backend via [Air](https://github.com/air-verse/air),
the frontend via `next dev`).

## Roadmap

Original plan vs. what actually got built, and what's still ahead:

1. ~~In-memory stock contention — mutexes, atomics~~ → built instead
   as a **single-writer goroutine** (Worker A) serializing access via
   channels, the CSP alternative to locking.
2. ~~Worker pools and rate limiting~~ → partially covered: the intake
   channel's bounded-size + reject-when-full is a basic backpressure
   mechanism. A true worker *pool* (multiple concurrent writers) isn't
   applicable here since stock mutation is deliberately single-writer.
3. **Messaging & async delivery (Kafka)** — not on the original list,
   but became the actual focus: producer/consumer semantics, consumer
   groups, at-least-once delivery with manual commits, a compensating
   "release" action, and a broadcast/fan-out pattern via SSE. Done.
4. **Context cancellation and timeouts under load** — partially in
   place (workers shut down cleanly via `context`, the fake
   confirmation delay is cancellation-aware); deeper timeout/backoff
   behavior under sustained load is still open.
5. **Persist the product catalog and enforce the stock invariant in
   Postgres** — done. The `products` table replaced the in-memory
   repository, `DecrementStock`/`ReleaseStock` became atomic
   `UPDATE ... WHERE stock >= $1` statements, and `StockWorker`
   became an actual worker *pool* (multiple goroutines safely calling
   the same atomic statement) -- this finished the "worker pools"
   item above too. Verified live: killing and restarting the backend
   no longer resets stock, and a burst of concurrent requests never
   oversells. The `orders`/`order_items`/`consumers` tables exist in
   `backend/db/schema.sql` but aren't wired into the checkout flow
   yet -- see step 7 below.
6. ~~Coordinate stock across multiple backend instances (Redis)~~ --
   this premise turned out not to survive contact with what got
   built: Postgres's row-level locking doesn't care whether concurrent
   callers are goroutines in one process or spread across multiple
   backend replicas -- the same atomic `UPDATE` stays correct either
   way. Redis isn't needed to fix a correctness gap here. Its real
   value would be **throughput** under very heavy contention on a
   single hot row (in-memory, single-threaded-per-command, faster
   than a Postgres row lock at scale) -- a genuinely different
   motivation, revisit only if/when that specific bottleneck actually
   shows up, not as a default "next step."

**Revisited 2026-09-09** — rather than reaching for new features
abstractly, we traced concrete reliability gaps that already exist in
the running async pipeline. These four are sequenced as one arc
(each depends on the one before it) and are now the focus, ahead of
the previously-planned Redis/CQRS work:

7. **Wire real orders into the checkout flow** — done. `Handler`
   creates a `pending` order (+ one `order_items` row) synchronously
   before enqueueing; `StockWorker` marks it `reserved`/`rejected`
   right where it already publishes `ReservationEvent`; `EventConsumer`
   marks it `approved`/`rejected` right where it already publishes
   `OrderStatusEvent` -- correlated by `orders.request_id`, not
   primary key. Deliberately no new Kafka consumer: extending the two
   components that already publish events, rather than adding a third
   consumer of the same topics, is what keeps step 8 below composable
   without a rewrite (the DB write and the Kafka publish now already
   live in the same component). `Order.status` is a state machine the
   aggregate enforces itself (see the "Order persistence" section
   above) -- terminal-state transitions are refused outright, a
   down payment on step 9. See `backend/internal/order`.
8. **Transactional outbox for the stock-decrement + event-publish
   dual-write** — `StockWorker.handleReservation` decrements stock in
   Postgres and publishes to Kafka as two independent calls
   (`worker.go:54-68`); a crash between them drops the event with
   stock already decremented and nothing downstream ever notified. A
   plain SQL transaction can't fix this -- it only spans Postgres, not
   Kafka -- so the fix is to shrink the atomic unit down to Postgres
   alone: write an `outbox` row in the same transaction as the
   order-status write step 7 already added to `StockWorker`, then a
   separate relay (poller or CDC) does the actual Kafka publish
   afterward. Not started.
9. **Idempotent consumers (dedup on redelivery)** — neither
   `EventConsumer.process` nor `StockWorker.handleRelease` guards
   against reprocessing the same message twice. Kafka's at-least-once
   guarantee only promises redelivery after a crash between
   `FetchMessage` and `CommitMessages` -- it says nothing about a
   handler's side effects being safe to repeat, and today they aren't:
   a crash there can double-release stock or emit a duplicate
   `OrderStatusEvent` on restart. This is what makes step 8's relay
   safe to retry -- dedup on the outbox row's id (e.g. a unique
   constraint on a `processed_events` table checked before applying a
   release or status change). Not started.
10. **Dead-letter / bounded retry for poison messages** — worth
    calling out precisely: `EventConsumer.Run` calls
    `CommitMessages` unconditionally after `process()` returns
    (`consumer.go:82-85`), regardless of whether decoding or
    publishing inside it failed. So today a bad message isn't
    "retried forever" -- it's silently dropped, offset and all, the
    one time it's seen. Real retry/DLQ behavior depends on step 9's
    idempotency work first, since retrying implies redelivery. Not
    started.
11. **Increase Kafka partitions per topic** — both topics are created
    with `--partitions 1`, so consumer groups today can never exercise
    the thing they exist for (parallel consumption across partitions,
    rebalancing). Revisit once ordering requirements are explicit
    (e.g. must all events for one product stay strictly ordered?).
    Not started.
12. **Cache invalidation under concurrent writes (Redis)** — a
    deliberately different motivation than step 6: not "make reads
    faster" but "what happens when a cached product's stock goes
    stale the instant a write happens underneath it." Framed this way
    it stays a concurrency lesson rather than a pure performance one.
    Not started.
13. **Read/write split (CQRS-style)** — well-motivated here (catalog
    reads vastly outnumber checkout writes during a real flash sale).
    Now clearly sequenced *after* steps 7-9: there's a real order
    lifecycle and reliable event delivery to project into a read
    model, rather than mixing persistence and replication concerns
    into the same pass. Not started.
14. **A real queueing / waiting-room UI** for traffic spikes — Worker
    B's fake latency already produces a visible backlog; an actual
    waiting-room experience on the frontend is still open.

See `docs/superpowers/specs/` for the design spec behind the initial
pass.

## Learning resources

- [`docs/learning/go-scheduler.md`](docs/learning/go-scheduler.md) —
  reading/watching list on the Go runtime scheduler (the GMP model,
  work-stealing, how goroutines get picked to receive from a shared
  channel), the background behind how the `StockWorker` pool behaves.
