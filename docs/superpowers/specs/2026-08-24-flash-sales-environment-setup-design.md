# flash-sales — environment setup design

Date: 2026-08-24

## Purpose

`flash-sales` is a study project for exploring concurrent systems at
scale and async execution contexts, using a limited-stock flash sale
as the running scenario (many concurrent buyers racing against a
fixed inventory). This document scopes the very first pass: a runnable
project skeleton, with no sale logic yet. Concurrency lessons (stock
contention, worker pools, rate limiting, distributed locks, queueing)
are built incrementally on top of this skeleton in later passes.

## Scope

In scope for this pass:
- Monorepo layout with a Go backend and a Next.js frontend
- A Dockerized local dev environment for both
- Git initialization
- A README documenting purpose, how to run the project, and a
  concurrency-topics roadmap for future passes

Out of scope for this pass (deferred to later, topic-driven passes):
- Any flash-sale domain logic (stock decrement, purchase endpoint, etc.)
- Postgres, Redis, or any message queue
- Authentication, deployment, CI

## Repo layout

```
flash-sales/
  backend/
    cmd/server/main.go
    go.mod
    Dockerfile.dev
    .air.toml
  frontend/               # create-next-app output (App Router, TS, Tailwind, ESLint)
    Dockerfile.dev
  docker-compose.yml
  README.md
  .gitignore
```

## Backend

- Go module, module path `flash-sales/backend`, Go 1.22+.
- Standard library only for HTTP (`net/http` with the Go 1.22+
  `ServeMux` pattern-based routing). No web framework — the point of
  this project is to work directly with goroutines, channels, `sync`,
  and atomics rather than have a framework abstract them away.
- `cmd/server/main.go` scaffolds a minimal server with a single
  `GET /health` endpoint returning `200 OK`, just enough to confirm
  the container boots and is reachable.
- `Dockerfile.dev` is based on a `golang` image, installs
  [Air](https://github.com/air-verse/air), and runs it as the
  container entrypoint so the server restarts automatically when
  `.go` files change under a bind-mounted `./backend`.
- `.air.toml` configures Air to watch `.go` files and rebuild
  `cmd/server`.

## Frontend

- Standard `create-next-app` scaffold: App Router, TypeScript,
  Tailwind CSS, ESLint. Default starter page, no custom UI yet — the
  frontend's role in this project is to visualize backend behavior
  (e.g. live stock count, a buy button that hammers the API) once
  sale logic exists, not to be the focus of study itself.
- `Dockerfile.dev` is based on a `node` image and runs `next dev`
  against a bind-mounted `./frontend`, so edits hot-reload as usual.
- Reads the backend's base URL from `NEXT_PUBLIC_API_URL`, set via
  `docker-compose.yml`.

## Local dev environment (Docker Compose)

`docker-compose.yml` at the repo root defines two services:

- `backend` — builds `backend/Dockerfile.dev`, mounts `./backend`,
  exposes port 8080, runs Air for live reload.
- `frontend` — builds `frontend/Dockerfile.dev`, mounts `./frontend`,
  exposes port 3000, runs `next dev`, gets
  `NEXT_PUBLIC_API_URL=http://localhost:8080` from the compose file.

No database or cache service is included yet. Those are added in
whichever future pass first needs them (e.g. Postgres when tackling
DB-backed stock contention, Redis when tackling distributed locks or
rate limiting) — introducing infrastructure only when there's a
concrete lesson driving it keeps each pass focused.

`docker compose up` becomes the single command to run the whole stack
locally.

## README.md

Documents:
- Project purpose and the study angle (concurrent/async systems via a
  flash-sale simulation)
- How to run the project (`docker compose up`, ports used)
- A rough roadmap of concurrency topics this project will work
  through in future passes, roughly in order:
  1. In-memory stock contention — mutexes, atomics
  2. Worker pools and rate limiting on the purchase endpoint
  3. Context cancellation and timeouts under load
  4. DB-backed contention — transactions, row locks (introduces Postgres)
  5. Distributed locking / caching (introduces Redis)
  6. Queueing / a virtual waiting room for traffic spikes

## Git

- `git init` at the repo root (already done).
- `.gitignore` covers Go build artifacts (e.g. compiled binaries,
  `tmp/` used by Air) and Node/Next.js artifacts (`node_modules/`,
  `.next/`, etc.).
- One initial commit once all scaffolding described above is in place.

## Testing

None beyond manual verification for this pass: both containers start
via `docker compose up`, `GET http://localhost:8080/health` returns
200, and the frontend starter page loads at `http://localhost:3000`.
There is no domain logic yet to unit test.

## Open questions

None — all decisions above were confirmed with the user during
brainstorming.
