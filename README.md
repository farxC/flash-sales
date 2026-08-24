# flash-sales

A study project for exploring **concurrent systems at scale and async
execution contexts**, using a limited-stock flash sale as the running
scenario: many concurrent buyers racing against a fixed inventory.

- **Backend**: Go, standard library only (`net/http`) — no framework,
  so the concurrency primitives (goroutines, channels, `sync`,
  atomics) stay front and center.
- **Frontend**: Next.js (App Router, TypeScript, Tailwind) — mainly a
  client for visualizing backend behavior (live stock counts, a buy
  button under load), not the focus of study.

## Running locally

```
docker compose up --build
```

- Backend: http://localhost:8081 (health check at `/health`)
- Frontend: http://localhost:3000

Both services bind-mount their source directories and hot-reload on
change (the Go backend via [Air](https://github.com/air-verse/air),
the frontend via `next dev`).

## Roadmap

This repo starts as a bare skeleton with no flash-sale logic yet.
Each concurrency topic below is added as its own pass, in roughly
this order:

1. **In-memory stock contention** — mutexes, atomics
2. **Worker pools and rate limiting** on the purchase endpoint
3. **Context cancellation and timeouts** under load
4. **DB-backed contention** — transactions, row locks (introduces Postgres)
5. **Distributed locking / caching** (introduces Redis)
6. **Queueing / a virtual waiting room** for traffic spikes

See `docs/superpowers/specs/` for the design spec behind each pass.
