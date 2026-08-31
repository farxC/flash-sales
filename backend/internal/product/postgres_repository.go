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
		SELECT id::text, name, description, value_in_cents, stock
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
		SELECT id::text, name, description, value_in_cents, stock
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

func (r *PostgresRepository) Save(ctx context.Context, p *Product) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE products
		SET name = $1, description = $2, value_in_cents = $3, stock = $4
		WHERE id = $5
	`, p.Name(), p.Description(), p.ValueInCents(), p.Stock(), p.ID())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrProductNotFound
	}
	return nil
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
		stock                 int
	)
	if err := row.Scan(&id, &name, &description, &valueInCents, &stock); err != nil {
		return nil, err
	}

	return NewProduct(id, name, description, valueInCents, stock)
}
