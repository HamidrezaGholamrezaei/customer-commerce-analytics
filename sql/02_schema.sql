
-- ============================================================
-- Schema: c360
-- Tables: dim_date, dim_item, dim_buyer, fact_sales
-- ============================================================

CREATE SCHEMA IF NOT EXISTS c360;

-- ------------------------------------------------------------
-- Dimension: Date
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS c360.dim_date(
    date_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    day SMALLINT NOT NULL CHECK (day BETWEEN 1 AND 31),
    is_weekend BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_dim_date_full_date ON c360.dim_date(full_date);

-- ------------------------------------------------------------
-- Dimension: Item
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS c360.dim_item(
    item_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    item_code TEXT NOT NULL UNIQUE,
    item_id BIGINT NOT NULL,
    item_name TEXT NOT NULL,
    category TEXT,
    version TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_dim_item_item_id ON c360.dim_item(item_id);
CREATE INDEX IF NOT EXISTS ix_dim_item_category ON c360.dim_item(category);

-- ------------------------------------------------------------
-- Dimension: Buyer
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS c360.dim_buyer(
    buyer_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    buyer_id BIGINT NOT NULL UNIQUE
);

-- ------------------------------------------------------------
-- Fact: Sales
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS c360.fact_sales(
    fact_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_key INT NOT NULL REFERENCES c360.dim_date(date_key),
    item_key BIGINT NOT NULL REFERENCES c360.dim_item(item_key),
    buyer_key BIGINT NOT NULL REFERENCES c360.dim_buyer(buyer_key),
    transaction_id BIGINT NOT NULL,
    final_quantity INTEGER NOT NULL,
    total_revenue NUMERIC(14,2) NOT NULL DEFAULT 0,
    price_reductions NUMERIC(14,2) NOT NULL DEFAULT 0,
    refunds NUMERIC(14,2) NOT NULL DEFAULT 0,
    final_revenue NUMERIC(14,2) NOT NULL DEFAULT 0,
    sales_tax NUMERIC(14,2) NOT NULL DEFAULT 0,
    overall_revenue NUMERIC(14,2) NOT NULL DEFAULT 0,
    refunded_item_count INTEGER NOT NULL,
    purchased_item_count INTEGER NOT NULL DEFAULT 0 CHECK (purchased_item_count >= 0),
    CONSTRAINT ck_revenue_1 CHECK (final_revenue = total_revenue + price_reductions + refunds),
    CONSTRAINT ck_revenue_2 CHECK (abs(overall_revenue - (final_revenue + sales_tax)) <= 0.01),
    CONSTRAINT ck_qty CHECK (final_quantity = purchased_item_count + refunded_item_count),
    CONSTRAINT ck_refunded CHECK (refunded_item_count <= 0)
);

CREATE INDEX IF NOT EXISTS ix_fact_sales_date_key ON c360.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_item_key ON c360.fact_sales(item_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_buyer_key ON c360.fact_sales(buyer_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_txn ON c360.fact_sales(transaction_id);
CREATE INDEX IF NOT EXISTS ix_fact_sales_buyer_date ON c360.fact_sales (buyer_key, date_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_item_date ON c360.fact_sales (item_key, date_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_txn_item ON c360.fact_sales (transaction_id, item_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_txn_buyer ON c360.fact_sales (transaction_id, buyer_key);
