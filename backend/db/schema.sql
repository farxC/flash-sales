-- Runs automatically on first init of the postgres container's data
-- volume (mounted into /docker-entrypoint-initdb.d/). Not re-run on
-- restarts -- only when the volume is created fresh.

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE consumers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  address TEXT NOT NULL,
  tax_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TRIGGER consumers_set_updated_at
  BEFORE UPDATE ON consumers
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE products (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  value_in_cents BIGINT NOT NULL CHECK (value_in_cents >= 0),
  stock INT NOT NULL CHECK (stock >= 0),
  -- Fixed at creation, never changes afterward. ReleaseStock checks
  -- against this so a release can never push stock past what the
  -- product actually started with.
  initial_stock INT NOT NULL CHECK (initial_stock >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TRIGGER products_set_updated_at
  BEFORE UPDATE ON products
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  consumer_id UUID NOT NULL REFERENCES consumers(id),
  -- Correlation key threaded through the whole checkout pipeline
  -- (checkout.Request, ReservationEvent, OrderStatusEvent all carry
  -- this same id) -- StockWorker and EventConsumer update this row
  -- via WHERE request_id = $1, never by primary key.
  request_id TEXT NOT NULL UNIQUE,
  total_value_in_cents BIGINT NOT NULL DEFAULT 0 CHECK (total_value_in_cents >= 0),
  status TEXT NOT NULL CHECK (status IN ('pending', 'reserved', 'approved', 'rejected')),
  paid_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX orders_consumer_id_idx ON orders(consumer_id);

CREATE TRIGGER orders_set_updated_at
  BEFORE UPDATE ON orders
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE order_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL REFERENCES orders(id),
  product_id UUID NOT NULL REFERENCES products(id),
  quantity INT NOT NULL CHECK (quantity > 0),
  value BIGINT NOT NULL CHECK (value >= 0), -- unit price snapshot at purchase time
  is_enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX order_items_order_id_idx ON order_items(order_id);
CREATE INDEX order_items_product_id_idx ON order_items(product_id);

CREATE TRIGGER order_items_set_updated_at
  BEFORE UPDATE ON order_items
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Seed data for local development.
INSERT INTO products (name, description, value_in_cents, stock, initial_stock)
VALUES (
  'Limited Edition Sneakers',
  'Only 100 pairs available in this flash sale.',
  1999900,
  100,
  100
);

-- Deliberately a single, fixed "guest" consumer -- buyer identity
-- and auth are out of scope for this project (the focus is
-- event-driven architecture, not e-commerce). Every order uses this
-- same consumer id, referenced from Go as order.GuestConsumerID.
--
-- CAUTION: like the products seed above, this only runs on a FRESH
-- postgres_data volume (see the file-level comment at the top). If
-- you already have a running local Postgres from before this
-- change, `docker compose down -v` (or manually run this INSERT and
-- the orders.request_id column addition) before the guest consumer
-- or request_id will exist.
INSERT INTO consumers (id, name, address, tax_id)
VALUES (
  '00000000-0000-0000-0000-000000000001',
  'Guest',
  'N/A',
  'N/A'
);
