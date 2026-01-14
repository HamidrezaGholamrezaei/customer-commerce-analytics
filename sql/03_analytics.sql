-- ============================================================
-- Schema: c360
-- Analytics Views
-- ============================================================

-- ------------------------------------------------------------
-- View: vw_date
-- Helper view with calendar attributes
-- ------------------------------------------------------------
DROP VIEW IF EXISTS c360.vw_date CASCADE;

CREATE VIEW c360.vw_date AS
SELECT
    d.date_key,
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    d.day,
    d.is_weekend,
    date_trunc('month', d.full_date)::date AS month_start,
    (date_trunc('month', d.full_date) + INTERVAL '1 month' - INTERVAL '1 day')::date AS month_end,
    to_char(d.full_date, 'YYYY-MM') AS year_month,
    to_char(d.full_date, 'Mon') AS month_name,
    EXTRACT(week FROM d.full_date)::int AS week_of_year,
    (d.full_date = date_trunc('month', d.full_date)::date) AS is_month_start,
    (d.full_date = (date_trunc('month', d.full_date) + INTERVAL '1 month' - INTERVAL '1 day')::date) AS is_month_end,
    EXTRACT(dow FROM d.full_date)::int AS day_of_week,
    to_char(d.full_date, 'Dy') AS day_name_short,
    trim(to_char(d.full_date, 'Day')) AS day_name
FROM c360.dim_date d;

-- ------------------------------------------------------------
-- View: vw_sales_daily
-- Daily revenue and orders
-- ------------------------------------------------------------
DROP VIEW IF EXISTS c360.vw_sales_daily CASCADE;

CREATE VIEW c360.vw_sales_daily AS
WITH daily AS (
    SELECT
        v.full_date,
        v.year,
        v.month,
        v.year_month,
        v.month_start,
        v.month_end,
        SUM(f.final_revenue) AS daily_revenue,
        COUNT(DISTINCT f.transaction_id) AS daily_orders
    FROM c360.vw_date v
    LEFT JOIN c360.fact_sales f
      ON f.date_key = v.date_key
    GROUP BY
        v.full_date,
        v.year,
        v.month,
        v.year_month,
        v.month_start,
        v.month_end
)
SELECT
    full_date,
    year,
    month,
    year_month,
    month_start,
    month_end,
    COALESCE(daily_revenue, 0) AS daily_revenue,
    COALESCE(daily_orders, 0)  AS daily_orders
FROM daily;

-- ------------------------------------------------------------
-- View: vw_clv_per_buyer
-- Buyer-level CLV
-- ------------------------------------------------------------
DROP VIEW IF EXISTS c360.vw_clv_per_buyer CASCADE;

CREATE VIEW c360.vw_clv_per_buyer AS
WITH asof AS (
    SELECT MAX(d.full_date) AS as_of_date
    FROM c360.fact_sales f
    JOIN c360.dim_date d ON d.date_key = f.date_key
),
base AS (
    SELECT
        b.buyer_key,
        b.buyer_id,
        SUM(f.final_revenue) AS total_revenue,
        COUNT(DISTINCT f.transaction_id) AS total_orders,
        MIN(d.full_date) AS first_purchase_date,
        MAX(d.full_date) AS last_purchase_date
    FROM c360.dim_buyer b
    LEFT JOIN c360.fact_sales f
      ON f.buyer_key = b.buyer_key
    LEFT JOIN c360.dim_date d
      ON f.date_key = d.date_key
    GROUP BY b.buyer_key, b.buyer_id
)
SELECT
    base.buyer_key,
    base.buyer_id,
    COALESCE(base.total_revenue, 0) AS total_revenue,
    COALESCE(base.total_orders, 0) AS total_orders,
    CASE
        WHEN COALESCE(base.total_orders, 0) > 0
            THEN base.total_revenue / NULLIF(base.total_orders, 0)
        ELSE NULL
    END AS avg_order_value,
    base.first_purchase_date,
    base.last_purchase_date,
    CASE
        WHEN base.last_purchase_date IS NOT NULL
            THEN (asof.as_of_date - base.last_purchase_date)
        ELSE NULL
    END AS days_since_last_purchase,
    CASE
        WHEN COALESCE(base.total_orders, 0) = 0 THEN FALSE
        WHEN base.last_purchase_date >= (asof.as_of_date - INTERVAL '30 days')::date THEN TRUE
        ELSE FALSE
    END AS is_active_30d,
    CASE
        WHEN COALESCE(base.total_orders, 0) = 0 THEN TRUE
        ELSE FALSE
    END AS is_never_purchased
FROM base
CROSS JOIN asof;

-- ------------------------------------------------------------
-- View: vw_return_rates_item
-- Return / refund metrics by item and month
-- ------------------------------------------------------------
DROP VIEW IF EXISTS c360.vw_return_rates_item CASCADE;

CREATE VIEW c360.vw_return_rates_item AS
WITH agg AS (
    SELECT
        i.item_key,
        i.item_code,
        i.item_name,
        i.category,
        v.year,
        v.month,
        v.year_month,
        SUM(f.purchased_item_count) AS purchased_qty,
        SUM(f.refunded_item_count) AS refunded_qty_raw,
        SUM(f.refunds) AS refunded_revenue
    FROM c360.fact_sales f
    JOIN c360.dim_item i
      ON f.item_key = i.item_key
    JOIN c360.vw_date v
      ON f.date_key = v.date_key
    GROUP BY
        i.item_key,
        i.item_code,
        i.item_name,
        i.category,
        v.year,
        v.month,
        v.year_month
)
SELECT
    item_key,
    item_code,
    item_name,
    category,
    year,
    month,
    year_month,
    purchased_qty,
    ABS(refunded_qty_raw) AS refunded_qty,
    refunded_revenue,
    CASE
        WHEN purchased_qty <> 0
            THEN ABS(refunded_qty_raw)::numeric / purchased_qty
        ELSE NULL
    END AS return_rate,
    -- Overall item-level return rate (all time)
    SUM(ABS(refunded_qty_raw)) OVER (PARTITION BY item_key)::numeric
        / NULLIF(SUM(purchased_qty) OVER (PARTITION BY item_key), 0) AS item_return_rate_overall
FROM agg;

-- ------------------------------------------------------------
-- View: vw_return_rates_category
-- Return / refund metrics by category and month
-- ------------------------------------------------------------
DROP VIEW IF EXISTS c360.vw_return_rates_category CASCADE;

CREATE VIEW c360.vw_return_rates_category AS
WITH agg AS (
    SELECT
        i.category,
        v.year,
        v.month,
        v.year_month,
        SUM(f.purchased_item_count) AS purchased_qty,
        SUM(f.refunded_item_count) AS refunded_qty_raw,
        SUM(f.refunds) AS refunded_revenue
    FROM c360.fact_sales f
    JOIN c360.dim_item i
      ON f.item_key = i.item_key
    JOIN c360.vw_date v
      ON f.date_key = v.date_key
    GROUP BY
        i.category,
        v.year,
        v.month,
        v.year_month
)
SELECT
    category,
    year,
    month,
    year_month,
    purchased_qty,
    ABS(refunded_qty_raw) AS refunded_qty,
    refunded_revenue,
    CASE
        WHEN purchased_qty <> 0
            THEN ABS(refunded_qty_raw)::numeric / purchased_qty
        ELSE NULL
    END AS return_rate
FROM agg;

-- ------------------------------------------------------------
-- View: vw_churn_cohorts
-- Cohorts by first purchase month and retention over months
-- ------------------------------------------------------------
DROP VIEW IF EXISTS c360.vw_churn_cohorts CASCADE;

CREATE VIEW c360.vw_churn_cohorts AS
WITH base AS (
    SELECT
        b.buyer_key,
        b.buyer_id,
        d.full_date,
        DATE_TRUNC('month', d.full_date)::date AS activity_month,
        MIN(d.full_date) OVER (PARTITION BY b.buyer_key) AS first_purchase_date
    FROM c360.fact_sales f
    JOIN c360.dim_buyer b
      ON f.buyer_key = b.buyer_key
    JOIN c360.dim_date d
      ON f.date_key = d.date_key
),
cohorted AS (
    SELECT DISTINCT
        buyer_key,
        buyer_id,
        DATE_TRUNC('month', first_purchase_date)::date AS cohort_month,
        activity_month
    FROM base
),
cohort_activity AS (
    SELECT
        cohort_month,
        activity_month,
        (
            (EXTRACT(YEAR FROM activity_month) * 12 + EXTRACT(MONTH FROM activity_month)) -
            (EXTRACT(YEAR FROM cohort_month) * 12 + EXTRACT(MONTH FROM cohort_month))
        )::int AS months_since_cohort,
        COUNT(DISTINCT buyer_key) AS active_buyers
    FROM cohorted
    GROUP BY
        cohort_month,
        activity_month
)
SELECT
    cohort_month,
    activity_month,
    months_since_cohort,
    active_buyers,
    MAX(active_buyers) FILTER (WHERE months_since_cohort = 0)
        OVER (PARTITION BY cohort_month) AS cohort_size,
    CASE
        WHEN MAX(active_buyers) FILTER (WHERE months_since_cohort = 0)
                 OVER (PARTITION BY cohort_month) > 0
        THEN active_buyers::numeric
             / MAX(active_buyers) FILTER (WHERE months_since_cohort = 0)
                 OVER (PARTITION BY cohort_month)
        ELSE NULL
    END AS retention_rate
FROM cohort_activity;

-- ------------------------------------------------------------
-- View: vw_churn_summary
-- Simple churn definition based on last purchase date
-- ------------------------------------------------------------
DROP VIEW IF EXISTS c360.vw_churn_summary CASCADE;

CREATE VIEW c360.vw_churn_summary AS
SELECT
    buyer_key,
    buyer_id,
    last_purchase_date,
    days_since_last_purchase,
    (days_since_last_purchase IS NOT NULL AND days_since_last_purchase > 30) AS is_churned_30d,
    (days_since_last_purchase IS NOT NULL AND days_since_last_purchase > 60) AS is_churned_60d,
    (last_purchase_date IS NULL) AS is_never_purchased
FROM c360.vw_clv_per_buyer;

-- ------------------------------------------------------------
-- View: vw_item_performance
-- Revenue, quantities, returns, and ranking per item
-- ------------------------------------------------------------
DROP VIEW IF EXISTS c360.vw_item_performance CASCADE;

CREATE VIEW c360.vw_item_performance AS
WITH base AS (
    SELECT
        i.item_key,
        i.item_code,
        i.item_name,
        i.category,
        SUM(f.final_revenue) AS total_revenue,
        SUM(f.final_quantity) AS total_quantity,
        SUM(f.purchased_item_count) AS total_purchased_qty,
        SUM(f.refunded_item_count) AS total_refunded_qty,
        SUM(f.price_reductions) AS total_discounts
    FROM c360.fact_sales f
    JOIN c360.dim_item i
      ON f.item_key = i.item_key
    GROUP BY
        i.item_key,
        i.item_code,
        i.item_name,
        i.category
),
enriched AS (
    SELECT
        item_key,
        item_code,
        item_name,
        category,
        total_revenue,
        total_quantity,
        total_refunded_qty,
        total_discounts,
        CASE
            WHEN total_purchased_qty <> 0
                THEN ABS(total_refunded_qty)::numeric / total_purchased_qty
            ELSE NULL
        END AS return_rate,
        CASE
            WHEN total_revenue <> 0
                THEN ABS(total_discounts)::numeric / total_revenue
            ELSE 0
        END AS discount_share,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
        RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS revenue_rank_in_category
    FROM base
)
SELECT *
FROM enriched;
