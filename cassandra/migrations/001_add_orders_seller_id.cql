-- Apply once when upgrading a database whose orders table lacks seller_id.
-- CREATE TABLE IF NOT EXISTS in init.cql does not alter existing tables.
ALTER TABLE ecommerce.orders ADD seller_id TEXT;
