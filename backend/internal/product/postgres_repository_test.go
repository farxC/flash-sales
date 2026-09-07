package product

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool connects to the Postgres instance started by
// `docker compose up postgres`, using POSTGRES_DSN. If it's not set,
// these tests are skipped -- plain `go test ./...` stays fast and
// dependency-free by default; run with POSTGRES_DSN set (matching
// docker-compose.yml's credentials, e.g.
// postgres://flashsales:flashsales@localhost:5432/flashsales?sslmode=disable)
// to exercise them.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set; skipping Postgres integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create postgres pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

// seedTestProduct inserts a product with the given stock directly via
// SQL (bypassing the repository, since we're testing the repository)
// and registers its cleanup.
func seedTestProduct(t *testing.T, pool *pgxpool.Pool, stock int) string {
	t.Helper()

	ctx := context.Background()
	var id string
	err := pool.QueryRow(ctx, `
		INSERT INTO products (name, description, value_in_cents, stock)
		VALUES ('test product', 'created by postgres_repository_test.go', 100, $1)
		RETURNING id::text
	`, stock).Scan(&id)
	if err != nil {
		t.Fatalf("failed to seed test product: %v", err)
	}

	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DELETE FROM products WHERE id = $1`, id); err != nil {
			t.Logf("failed to clean up test product %s: %v", id, err)
		}
	})

	return id
}

func getStock(t *testing.T, pool *pgxpool.Pool, id string) int {
	t.Helper()

	var stock int
	if err := pool.QueryRow(context.Background(), `SELECT stock FROM products WHERE id = $1`, id).Scan(&stock); err != nil {
		t.Fatalf("failed to read stock for %s: %v", id, err)
	}
	return stock
}

// TestPostgresRepository_DecrementStock_NeverOversells is the
// automated version of every manual burst test run by hand with curl
// throughout this project: many goroutines race to decrement the
// same row, and the invariant must hold regardless of who wins.
func TestPostgresRepository_DecrementStock_NeverOversells(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)

	const initialStock = 10
	const concurrentRequests = 50

	id := seedTestProduct(t, pool, initialStock)

	var succeeded, insufficientStock int64
	var wg sync.WaitGroup
	wg.Add(concurrentRequests)
	for range concurrentRequests {
		go func() {
			defer wg.Done()
			err := repo.DecrementStock(context.Background(), id, 1)
			switch err {
			case nil:
				atomic.AddInt64(&succeeded, 1)
			case ErrInsufficientStock:
				atomic.AddInt64(&insufficientStock, 1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if succeeded != initialStock {
		t.Errorf("succeeded = %d, want %d (must never oversell or undersell)", succeeded, initialStock)
	}
	if want := int64(concurrentRequests - initialStock); insufficientStock != want {
		t.Errorf("insufficientStock = %d, want %d", insufficientStock, want)
	}

	finalStock := getStock(t, pool, id)
	if finalStock != 0 {
		t.Errorf("final stock = %d, want 0", finalStock)
	}
	if finalStock < 0 {
		t.Fatalf("final stock went negative: %d -- the invariant this whole project is about was violated", finalStock)
	}
}

func TestPostgresRepository_DecrementStock_ProductNotFound(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)

	err := repo.DecrementStock(context.Background(), "00000000-0000-0000-0000-000000000000", 1)
	if err != ErrProductNotFound {
		t.Fatalf("got err %v, want %v", err, ErrProductNotFound)
	}
}

// TestPostgresRepository_ReleaseStock_NeverLosesAnUpdate proves the
// increment is also atomic under concurrency -- without that, this
// is exactly the kind of lost-update race that DecrementStock had
// before it became a single atomic statement.
func TestPostgresRepository_ReleaseStock_NeverLosesAnUpdate(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)

	const initialStock = 0
	const concurrentReleases = 50

	id := seedTestProduct(t, pool, initialStock)

	var wg sync.WaitGroup
	wg.Add(concurrentReleases)
	for range concurrentReleases {
		go func() {
			defer wg.Done()
			if err := repo.ReleaseStock(context.Background(), id, 1); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	finalStock := getStock(t, pool, id)
	if finalStock != concurrentReleases {
		t.Errorf("final stock = %d, want %d (a lost update would undercount this)", finalStock, concurrentReleases)
	}
}

func TestPostgresRepository_ReleaseStock_ProductNotFound(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)

	err := repo.ReleaseStock(context.Background(), "00000000-0000-0000-0000-000000000000", 1)
	if err != ErrProductNotFound {
		t.Fatalf("got err %v, want %v", err, ErrProductNotFound)
	}
}
