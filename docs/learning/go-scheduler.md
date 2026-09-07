# Go runtime scheduler — reading & watching list

Background material for understanding *why* the worker pool in
`backend/internal/checkout/worker.go` behaves the way it does: how
the Go runtime decides which goroutine runs when, and which one of
several waiting goroutines receives the next value sent on a shared
channel (see `StockWorker.Run`).

## Read, in this order

1. [Scheduling In Go : Part I - OS Scheduler](https://www.ardanlabs.com/blog/2018/08/scheduling-in-go-part1.html)
   -- how the *operating system's* scheduler works, the necessary
   foundation before the Go-specific parts.
2. [Scheduling In Go : Part II - Go Scheduler](https://www.ardanlabs.com/blog/2018/08/scheduling-in-go-part2.html)
   -- the actual Go scheduler internals: the GMP model (Goroutines,
   Machines/OS threads, Processors), work-stealing, handling blocking
   syscalls.
3. [Scheduling In Go : Part III - Concurrency](https://www.ardanlabs.com/blog/2018/12/scheduling-in-go-part3.html)
   -- applies the model to real concurrency patterns; CPU-bound vs.
   IO-bound workloads.
4. [Scalable Go Scheduler Design Doc](https://docs.google.com/document/d/1TTj4T2JO42uD5ID9e89oa0sLKhJYD0Y_kqxDv3I3XMw/edit)
   (Dmitry Vyukov, 2012) -- the original proposal that introduced the
   current G-M-P architecture. Terser and more technical; best read
   after 1-3, not first.

## Watch

- [Kavya Joshi -- The Scheduler Saga](https://www.youtube.com/watch?v=YHRO5WQGh0k)
  (GopherCon 2018, ~30 min) -- the best single scheduler-focused talk;
  a visual walkthrough of goroutines moving between run queues.
- [Dmitry Vyukov -- Go scheduler: Implementing language with lightweight concurrency](https://www.youtube.com/watch?v=-K11rY57K7k)
  -- straight from the person who designed the current GMP model.
- [Bill Kennedy -- Go's Trace Tooling and Concurrency](https://www.youtube.com/watch?v=Gqo0oCfZSjg)
  (GopherCon 2025) -- more recent, covers the scheduler through the
  lens of actually tracing/observing it.

No single dedicated "Go scheduler" YouTube playlist exists -- the
talks above are individual videos. GopherCon's full-conference
playlists have other concurrency-adjacent talks each year (channels,
goroutine leaks, context, etc.) if you want to browse further:
[2023](https://www.youtube.com/playlist?list=PL2ntRZ1ySWBep6rEAtp9jI6GXGZdlJmWN),
[2024](https://www.youtube.com/playlist?list=PL2ntRZ1ySWBdtH-tLdfcDJaWABxySlkRj),
[2025](https://www.youtube.com/playlist?list=PL-tb3yiDrcKr9V_ojBXfkCu267QrhPii2).

## Hands-on, with this repo

Run the backend with scheduler tracing enabled and fire a burst of
concurrent `POST /checkout` requests at it (see the burst-test
commands used throughout this project's history) while watching the
output -- a direct way to see the scheduler reacting to the same
worker-pool load this project is built around:

```sh
GODEBUG=schedtrace=1000 go run ./cmd/server
```

## Go all the way down (optional, advanced)

The scheduler's actual implementation lives in Go's own source at
`src/runtime/proc.go`, which has a long doc comment at the top
explaining the design in the maintainers' own words. Best read after
the above material gives you the vocabulary for it.
