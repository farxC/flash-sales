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
(non-empty name, non-negative price/stock), and a repository
abstraction currently backed by a single in-memory, hardcoded product.

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

### Endpoints

| Endpoint | Method | Notes |
|---|---|---|
| `/health` | GET | liveness check |
| `/products` | GET | list of products (currently one, hardcoded) |
| `/checkout` | POST | `{productId, quantity}` → `202 {requestId}`, or `400`/`404`/`503` |
| `/events` | GET | Server-Sent Events stream of `OrderStatusEvent`s |

## Running locally

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
5. **Persist orders and the product catalog (Postgres)** — the most
   urgent gap: Kafka guarantees the *events* survive a crash, but the
   *current state* (remaining stock, what got bought) doesn't --
   proven directly by killing the backend mid-request and watching
   stock reset to 100. This also sets up a direct, side-by-side
   comparison of three different answers to the same concurrency
   problem: the in-process single-writer goroutine (already built),
   a Postgres transaction/row lock (this step), and a Redis
   distributed lock (next step). Not started.
6. **Coordinate stock across multiple backend instances (Redis)** —
   depends on step 5. Worker A is safe today only because there is
   exactly **one** process running **one** goroutine that ever
   touches `Product.stock`. That stops being true the moment you run
   more than one backend replica (which a flash sale "at scale"
   realistically would) -- each replica would have its own
   uncoordinated `StockWorker`. Redis (an atomic `DECR`, or a
   distributed lock) is what replaces the single-writer invariant
   once "single process" is no longer the case. Not started.
7. **Cache invalidation under concurrent writes (Redis)** — a
   deliberately different motivation than step 6: not "make reads
   faster" but "what happens when a cached product's stock goes stale
   the instant a write happens underneath it." Framed this way it
   stays a concurrency lesson rather than a pure performance one. Not
   started.
8. **Read/write split (CQRS-style)** — well-motivated here (catalog
   reads vastly outnumber checkout writes during a real flash sale),
   but deliberately sequenced *after* step 5 so it doesn't mix two
   new kinds of complexity (persistence, then replication/eventual
   consistency) into the same pass. Not started.
9. **A real queueing / waiting-room UI** for traffic spikes — Worker
   B's fake latency already produces a visible backlog; an actual
   waiting-room experience on the frontend is still open.

See `docs/superpowers/specs/` for the design spec behind the initial
pass.
