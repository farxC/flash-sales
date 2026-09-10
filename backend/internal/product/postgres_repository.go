package product

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepository is a Repository backed by the products table.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) List(ctx context.Context) ([]*Product, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id::text, name, description, value_in_cents, stock, initial_stock
		FROM products
		ORDER BY created_at
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var products []*Product
	for rows.Next() {
		p, err := scanProduct(rows)
		if err != nil {
			return nil, err
		}
		products = append(products, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return products, nil
}

func (r *PostgresRepository) FindByID(ctx context.Context, id string) (*Product, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id::text, name, description, value_in_cents, stock, initial_stock
		FROM products
		WHERE id = $1
	`, id)

	p, err := scanProduct(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrProductNotFound
	}
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (r *PostgresRepository) DecrementStock(ctx context.Context, id string, qty int) error {
	// A plain UPDATE ... WHERE stock >= $1 can't tell "not enough
	// stock" apart from "no such product" -- both leave RowsAffected
	// at 0. This writable CTE resolves that in the same atomic
	// statement: the UPDATE never deletes a row, so checking EXISTS
	// against the table afterward reflects the product's existence
	// independent of whether the conditional decrement fired.
	var decremented int
	var exists bool
	err := r.pool.QueryRow(ctx, `
		WITH updated AS (
			UPDATE products
			SET stock = stock - $1
			WHERE id = $2 AND stock >= $1
			RETURNING id
		)
		SELECT
			(SELECT COUNT(*) FROM updated),
			EXISTS(SELECT 1 FROM products WHERE id = $2)
	`, qty, id).Scan(&decremented, &exists)
	if err != nil {
		return err
	}
	if decremented > 0 {
		return nil
	}
	if !exists {
		return ErrProductNotFound
	}
	return ErrInsufficientStock
}

func (r *PostgresRepository) ReleaseStock(ctx context.Context, id string, qty int) error {
	// Same ambiguity as DecrementStock, same fix: a writable CTE lets
	// one atomic statement distinguish "no such product" from "this
	// release would exceed initial_stock" -- the UPDATE never deletes
	// a row, so EXISTS afterward reflects existence either way.
	var released int
	var exists bool
	err := r.pool.QueryRow(ctx, `
		WITH updated AS (
			UPDATE products
			SET stock = stock + $1
			WHERE id = $2 AND stock + $1 <= initial_stock
			RETURNING id
		)
		SELECT
			(SELECT COUNT(*) FROM updated),
			EXISTS(SELECT 1 FROM products WHERE id = $2)
	`, qty, id).Scan(&released, &exists)
	if err != nil {
		return err
	}
	if released > 0 {
		return nil
	}
	if !exists {
		return ErrProductNotFound
	}
	return ErrReleaseExceedsInitialStock
}

// rowScanner covers both pgx.Row (QueryRow) and pgx.Rows (Query),
// which share this method but not a common interface in pgx.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanProduct(row rowScanner) (*Product, error) {
	var (
		id, name, description string
		valueInCents          int64
		stock, initialStock   int
	)
	if err := row.Scan(&id, &name, &description, &valueInCents, &stock, &initialStock); err != nil {
		return nil, err
	}

	return RehydrateProduct(id, name, description, valueInCents, stock, initialStock)
}
