package order

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepository is a Repository backed by the orders/order_items
// tables.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

// uniqueViolation is Postgres's SQLSTATE for a unique constraint
// violation -- used here to recognize a duplicate request_id.
const uniqueViolation = "23505"

func (r *PostgresRepository) Create(ctx context.Context, o *Order) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var orderID string
	err = tx.QueryRow(ctx, `
		INSERT INTO orders (consumer_id, request_id, total_value_in_cents, status)
		VALUES ($1, $2, $3, $4)
		RETURNING id::text
	`, o.ConsumerID(), o.RequestID(), o.TotalValueInCents(), string(o.Status())).Scan(&orderID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return ErrDuplicateRequestID
		}
		return err
	}

	for _, item := range o.Items() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO order_items (order_id, product_id, quantity, value)
			VALUES ($1, $2, $3, $4)
		`, orderID, item.ProductID(), item.Quantity(), item.ValueInCents()); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (r *PostgresRepository) MarkReserved(ctx context.Context, requestID string) error {
	return r.transition(ctx, requestID, string(StatusPending), string(StatusReserved))
}

func (r *PostgresRepository) MarkApproved(ctx context.Context, requestID string) error {
	return r.transition(ctx, requestID, string(StatusReserved), string(StatusApproved))
}

func (r *PostgresRepository) MarkRejected(ctx context.Context, requestID, reason string) error {
	// Two valid predecessor statuses (pending -- immediate
	// out-of-stock, or reserved -- rejected during confirmation), so
	// this doesn't fit the single-predecessor shape transition()
	// handles. Same writable-CTE + EXISTS pattern otherwise.
	var updated int
	var exists bool
	err := r.pool.QueryRow(ctx, `
		WITH updated AS (
			UPDATE orders
			SET status = 'rejected'
			WHERE request_id = $1 AND status IN ('pending', 'reserved')
			RETURNING id
		)
		SELECT
			(SELECT COUNT(*) FROM updated),
			EXISTS(SELECT 1 FROM orders WHERE request_id = $1)
	`, requestID).Scan(&updated, &exists)
	if err != nil {
		return err
	}
	if updated > 0 {
		return nil
	}
	if !exists {
		return ErrOrderNotFound
	}
	return ErrInvalidTransition
}

// transition applies a single-predecessor status change, atomically
// distinguishing "no such order" from "illegal transition attempted"
// the same way product.PostgresRepository's DecrementStock/
// ReleaseStock distinguish "no such product" from "invariant
// violated" -- a writable CTE that never deletes a row, so EXISTS
// against the base table afterward reflects existence independent of
// whether the conditional UPDATE fired.
func (r *PostgresRepository) transition(ctx context.Context, requestID, from, to string) error {
	var updated int
	var exists bool
	err := r.pool.QueryRow(ctx, `
		WITH updated AS (
			UPDATE orders
			SET status = $3
			WHERE request_id = $1 AND status = $2
			RETURNING id
		)
		SELECT
			(SELECT COUNT(*) FROM updated),
			EXISTS(SELECT 1 FROM orders WHERE request_id = $1)
	`, requestID, from, to).Scan(&updated, &exists)
	if err != nil {
		return err
	}
	if updated > 0 {
		return nil
	}
	if !exists {
		return ErrOrderNotFound
	}
	return ErrInvalidTransition
}

func (r *PostgresRepository) FindByRequestID(ctx context.Context, requestID string) (*Order, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id::text, request_id, consumer_id::text, status
		FROM orders
		WHERE request_id = $1
	`, requestID)

	var id, reqID, consumerID, status string
	if err := row.Scan(&id, &reqID, &consumerID, &status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrOrderNotFound
		}
		return nil, err
	}

	rows, err := r.pool.Query(ctx, `
		SELECT product_id::text, quantity, value
		FROM order_items
		WHERE order_id = $1
	`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []Item
	for rows.Next() {
		var productID string
		var quantity int
		var value int64
		if err := rows.Scan(&productID, &quantity, &value); err != nil {
			return nil, err
		}
		item, err := NewItem(productID, quantity, value)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return RehydrateOrder(id, reqID, consumerID, items, Status(status))
}
