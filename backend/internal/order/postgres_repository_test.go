package order

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool connects to the Postgres instance started by
// `docker compose up postgres`, using POSTGRES_DSN. If it's not set,
// these tests are skipped -- plain `go test ./...` stays fast and
// dependency-free by default; run with POSTGRES_DSN set (matching
// docker-compose.yml's credentials, e.g.
// postgres://flashsales:flashsales@localhost:5432/flashsales?sslmode=disable)
// to exercise them. Requires the guest consumer seed row and the
// orders.request_id column from schema.sql -- if your local Postgres
// volume predates those, `docker compose down -v` first.
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

// seedTestProduct inserts a product directly via SQL, for order_items'
// foreign key, and registers its cleanup.
func seedTestProduct(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()

	ctx := context.Background()
	var id string
	err := pool.QueryRow(ctx, `
		INSERT INTO products (name, description, value_in_cents, stock, initial_stock)
		VALUES ('test product', 'created by order/postgres_repository_test.go', 500, 10, 10)
		RETURNING id::text
	`).Scan(&id)
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

// deleteTestOrder removes an order and its order_items, registered
// via t.Cleanup wherever a test creates an order row. order_items has
// no ON DELETE CASCADE on its order_id foreign key, so order_items
// must be deleted first -- deleting orders alone fails with a FK
// violation (silently, unless run with -v, since a failing cleanup
// only calls t.Logf) and leaves rows behind to collide with the next
// run via the request_id unique constraint.
func deleteTestOrder(t *testing.T, pool *pgxpool.Pool, orderID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `DELETE FROM order_items WHERE order_id = $1`, orderID); err != nil {
		t.Logf("failed to clean up order_items for order %s: %v", orderID, err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM orders WHERE id = $1`, orderID); err != nil {
		t.Logf("failed to clean up test order %s: %v", orderID, err)
	}
}

// seedTestOrder inserts an order (and one order_item) directly via
// SQL, bypassing the repository under test, in the given status.
func seedTestOrder(t *testing.T, pool *pgxpool.Pool, productID, requestID string, status Status) {
	t.Helper()

	ctx := context.Background()
	var orderID string
	err := pool.QueryRow(ctx, `
		INSERT INTO orders (consumer_id, request_id, total_value_in_cents, status)
		VALUES ($1, $2, $3, $4)
		RETURNING id::text
	`, GuestConsumerID, requestID, 1000, string(status)).Scan(&orderID)
	if err != nil {
		t.Fatalf("failed to seed test order: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO order_items (order_id, product_id, quantity, value)
		VALUES ($1, $2, 2, 500)
	`, orderID, productID); err != nil {
		t.Fatalf("failed to seed test order item: %v", err)
	}

	t.Cleanup(func() { deleteTestOrder(t, pool, orderID) })
}

func getOrderStatus(t *testing.T, pool *pgxpool.Pool, requestID string) string {
	t.Helper()

	var status string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM orders WHERE request_id = $1`, requestID).Scan(&status); err != nil {
		t.Fatalf("failed to read status for %s: %v", requestID, err)
	}
	return status
}

// deleteTestOrderByRequestID is deleteTestOrder's counterpart for
// tests that only know a request_id (repo.Create doesn't hand the
// generated order id back) -- same order_items-before-orders
// ordering, to avoid the FK-violation cleanup bug described above.
func deleteTestOrderByRequestID(t *testing.T, pool *pgxpool.Pool, requestID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE request_id = $1)
	`, requestID); err != nil {
		t.Logf("failed to clean up order_items for request %s: %v", requestID, err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM orders WHERE request_id = $1`, requestID); err != nil {
		t.Logf("failed to clean up test order for request %s: %v", requestID, err)
	}
}

func TestPostgresRepository_Create_PersistsOrderAndItems(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)
	productID := seedTestProduct(t, pool)

	item := mustItem(t, productID, 2, 500)
	o, err := NewOrder("req-create-1", GuestConsumerID, []Item{item})
	if err != nil {
		t.Fatalf("unexpected error building order: %v", err)
	}

	if err := repo.Create(context.Background(), o); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { deleteTestOrderByRequestID(t, pool, "req-create-1") })

	var consumerID, status string
	var total int64
	err = pool.QueryRow(context.Background(), `
		SELECT consumer_id::text, total_value_in_cents, status FROM orders WHERE request_id = $1
	`, "req-create-1").Scan(&consumerID, &total, &status)
	if err != nil {
		t.Fatalf("failed to read back order: %v", err)
	}
	if consumerID != GuestConsumerID {
		t.Errorf("consumer_id = %q, want %q", consumerID, GuestConsumerID)
	}
	if total != 1000 {
		t.Errorf("total_value_in_cents = %d, want 1000", total)
	}
	if status != string(StatusPending) {
		t.Errorf("status = %q, want %q", status, StatusPending)
	}

	var itemCount int
	var quantity int
	var value int64
	err = pool.QueryRow(context.Background(), `
		SELECT COUNT(*), MIN(quantity), MIN(value) FROM order_items oi
		JOIN orders o ON o.id = oi.order_id
		WHERE o.request_id = $1
	`, "req-create-1").Scan(&itemCount, &quantity, &value)
	if err != nil {
		t.Fatalf("failed to read back order_items: %v", err)
	}
	if itemCount != 1 {
		t.Fatalf("order_items count = %d, want 1", itemCount)
	}
	if quantity != 2 || value != 500 {
		t.Errorf("item = {quantity: %d, value: %d}, want {2, 500}", quantity, value)
	}
}

func TestPostgresRepository_Create_DuplicateRequestID(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)
	productID := seedTestProduct(t, pool)

	item := mustItem(t, productID, 1, 500)
	o1, _ := NewOrder("req-dup-1", GuestConsumerID, []Item{item})
	if err := repo.Create(context.Background(), o1); err != nil {
		t.Fatalf("unexpected error on first create: %v", err)
	}
	t.Cleanup(func() { deleteTestOrderByRequestID(t, pool, "req-dup-1") })

	o2, _ := NewOrder("req-dup-1", GuestConsumerID, []Item{item})
	if err := repo.Create(context.Background(), o2); err != ErrDuplicateRequestID {
		t.Fatalf("got err %v, want %v", err, ErrDuplicateRequestID)
	}
}

func TestPostgresRepository_MarkReserved_FromPending_Succeeds(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)
	productID := seedTestProduct(t, pool)
	seedTestOrder(t, pool, productID, "req-reserve-1", StatusPending)

	if err := repo.MarkReserved(context.Background(), "req-reserve-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := getOrderStatus(t, pool, "req-reserve-1"); got != string(StatusReserved) {
		t.Errorf("status = %q, want %q", got, StatusReserved)
	}
}

func TestPostgresRepository_MarkReserved_NotFound(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)

	if err := repo.MarkReserved(context.Background(), "req-does-not-exist"); err != ErrOrderNotFound {
		t.Fatalf("got err %v, want %v", err, ErrOrderNotFound)
	}
}

func TestPostgresRepository_MarkReserved_FromWrongStatus_ReturnsErrInvalidTransition(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)
	productID := seedTestProduct(t, pool)
	seedTestOrder(t, pool, productID, "req-reserve-2", StatusApproved)

	if err := repo.MarkReserved(context.Background(), "req-reserve-2"); err != ErrInvalidTransition {
		t.Fatalf("got err %v, want %v", err, ErrInvalidTransition)
	}
	if got := getOrderStatus(t, pool, "req-reserve-2"); got != string(StatusApproved) {
		t.Errorf("status changed to %q, want unchanged %q", got, StatusApproved)
	}
}

func TestPostgresRepository_MarkApproved_FromReserved_Succeeds(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)
	productID := seedTestProduct(t, pool)
	seedTestOrder(t, pool, productID, "req-approve-1", StatusReserved)

	if err := repo.MarkApproved(context.Background(), "req-approve-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := getOrderStatus(t, pool, "req-approve-1"); got != string(StatusApproved) {
		t.Errorf("status = %q, want %q", got, StatusApproved)
	}
}

func TestPostgresRepository_MarkRejected_FromPending_Succeeds(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)
	productID := seedTestProduct(t, pool)
	seedTestOrder(t, pool, productID, "req-reject-1", StatusPending)

	if err := repo.MarkRejected(context.Background(), "req-reject-1", "out of stock"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := getOrderStatus(t, pool, "req-reject-1"); got != string(StatusRejected) {
		t.Errorf("status = %q, want %q", got, StatusRejected)
	}
}

func TestPostgresRepository_MarkRejected_FromReserved_Succeeds(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)
	productID := seedTestProduct(t, pool)
	seedTestOrder(t, pool, productID, "req-reject-2", StatusReserved)

	if err := repo.MarkRejected(context.Background(), "req-reject-2", "rejected during confirmation"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := getOrderStatus(t, pool, "req-reject-2"); got != string(StatusRejected) {
		t.Errorf("status = %q, want %q", got, StatusRejected)
	}
}

func TestPostgresRepository_MarkRejected_FromTerminalState_Fails(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)
	productID := seedTestProduct(t, pool)
	seedTestOrder(t, pool, productID, "req-reject-3", StatusApproved)

	if err := repo.MarkRejected(context.Background(), "req-reject-3", "too late"); err != ErrInvalidTransition {
		t.Fatalf("got err %v, want %v", err, ErrInvalidTransition)
	}
	if got := getOrderStatus(t, pool, "req-reject-3"); got != string(StatusApproved) {
		t.Errorf("status changed to %q, want unchanged %q", got, StatusApproved)
	}
}

func TestPostgresRepository_FindByRequestID_RoundTrips(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)
	productID := seedTestProduct(t, pool)

	item := mustItem(t, productID, 3, 700)
	o, _ := NewOrder("req-roundtrip-1", GuestConsumerID, []Item{item})
	if err := repo.Create(context.Background(), o); err != nil {
		t.Fatalf("unexpected error creating: %v", err)
	}
	t.Cleanup(func() { deleteTestOrderByRequestID(t, pool, "req-roundtrip-1") })

	found, err := repo.FindByRequestID(context.Background(), "req-roundtrip-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found.RequestID() != "req-roundtrip-1" {
		t.Errorf("requestID = %q, want %q", found.RequestID(), "req-roundtrip-1")
	}
	if found.ConsumerID() != GuestConsumerID {
		t.Errorf("consumerID = %q, want %q", found.ConsumerID(), GuestConsumerID)
	}
	if found.Status() != StatusPending {
		t.Errorf("status = %q, want %q", found.Status(), StatusPending)
	}
	if found.TotalValueInCents() != 2100 {
		t.Errorf("total = %d, want 2100", found.TotalValueInCents())
	}
	if len(found.Items()) != 1 || found.Items()[0].ProductID() != productID || found.Items()[0].Quantity() != 3 {
		t.Errorf("unexpected items: %+v", found.Items())
	}
}

func TestPostgresRepository_FindByRequestID_NotFound(t *testing.T) {
	pool := testPool(t)
	repo := NewPostgresRepository(pool)

	if _, err := repo.FindByRequestID(context.Background(), "req-does-not-exist"); err != ErrOrderNotFound {
		t.Fatalf("got err %v, want %v", err, ErrOrderNotFound)
	}
}
