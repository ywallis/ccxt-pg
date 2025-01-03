-- Checks for the existence of a public trades table with a Exchange | Symbol name and
-- creates it if it doesn't exist
    CREATE TABLE IF NOT EXISTS {} (
    db_id SERIAL PRIMARY KEY,
    created_at timestamp(6) NOT NULL DEFAULT now(),
    info text,
    id text,
    timestamp int8,
    datetime timestamptz(6),
    symbol text,
    order_id text,
    type text,
    side text,
    takerormaker text,
    price numeric,
    amount numeric,
    cost numeric,
    fee text,
    fees text
);
