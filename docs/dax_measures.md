# DAX Measures

These are the main DAX measures used in the semantic model (not all measures, but only the core ones).

---

## Revenue & Demand

- **Total Revenue:**
```DAX
Total Revenue = SUM (vw_sales_daily[daily_revenue])
```

- **Revenue (7D Rolling):**
```DAX
Revenue 7D Rolling =
CALCULATE (
    [Total Revenue],
    DATESINPERIOD (dim_date[full_date], MAX (dim_date[full_date]), -7, DAY)
)
```

- **Revenue (Prior 7D Rolling):**
```DAX
Revenue Prior 7D Rolling =
CALCULATE (
    [Total Revenue],
    DATESINPERIOD (dim_date[full_date], MAX (dim_date[full_date]) - 7, -7, DAY)
)
```

- **Total Orders:**
```DAX
Total Orders = SUM (vw_sales_daily[daily_orders])
```

- **Orders (7D Rolling):**
```DAX
Orders 7D Rolling =
CALCULATE (
    [Total Orders],
    DATESINPERIOD (dim_date[full_date],MAX (dim_date[full_date]), -7, DAY)
)
```

- **Orders (Prior 7D Rolling):**
```DAX
Orders Prior 7D Rolling =
CALCULATE (
    [Total Orders],
    DATESINPERIOD (dim_date[full_date], MAX (dim_date[full_date]) - 7, -7, DAY)
)
```

- **Avg Order Value (7D Rolling):**
```DAX
AOV 7D Rolling = DIVIDE ([Revenue 7D Rolling], [Orders 7D Rolling])
```

- **Discount Share:**
```DAX
Discount Share = AVERAGE (vw_item_performance[discount_share])
```

---

## Customers

- **Total Buyers:**
```DAX
Total Buyers = DISTINCTCOUNT (dim_buyer[buyer_id])
```

- **Active Buyers (7D):**
```DAX
Active Buyers 7D =
CALCULATE (
    DISTINCTCOUNT (vw_clv_per_buyer[buyer_id]),
    FILTER (
        vw_clv_per_buyer,
        vw_clv_per_buyer[last_purchase_date] >= MAX (dim_date[full_date]) - 7
            && vw_clv_per_buyer[last_purchase_date] <= MAX (dim_date[full_date])
    )
)
```

- **Active Buyers (Prior 7D):**
```DAX
Active Buyers Prior 7D =
CALCULATE (
    DISTINCTCOUNT (vw_clv_per_buyer[buyer_id]),
    FILTER (
        vw_clv_per_buyer,
        vw_clv_per_buyer[last_purchase_date] >= (MAX (dim_date[full_date]) - 7) - 7
            && vw_clv_per_buyer[last_purchase_date] <= (MAX (dim_date[full_date]) - 7)
    )
)
```

- **Churned Buyers (30D):**
```DAX
Churned Buyers 30D =
COUNTROWS (
    FILTER (vw_churn_summary, vw_churn_summary[is_churned_30d] = "t")
)
```

- **Churn Rate (30D):**
```DAX
Churn Rate 30D = DIVIDE ([Churned Buyers 30D], [Total Buyers])
```

---


## Returns

- **Purchased Qty (Item):**
```DAX
Purchased Qty Item = SUM (vw_return_rates_item[purchased_qty])
```

- **Returned Qty (Item):**
```DAX
Returned Qty Item = SUM (vw_return_rates_item[refunded_qty])
```

- **Return Rate (Item):**
```DAX
Return Rate Item = DIVIDE ([Returned Qty Item], [Purchased Qty Item])
```

- **Purchased Qty (Category):**
```DAX
Purchased Qty Category = SUM (vw_return_rates_category[purchased_qty])
```

- **Returned Qty (Category):**
```DAX
Returned Qty Category = SUM (vw_return_rates_category[refunded_qty])
```

- **Return Rate (Category):**
```DAX
Return Rate Category = DIVIDE ([Returned Qty Category], [Purchased Qty Category])
```

---

## Promotions

- **Promo Revenue:**
```DAX
Promo Revenue =
CALCULATE (
    SUM (vw_promo_revenue_daily[revenue]), vw_promo_revenue_daily[is_promo] = "t"
)
```

- **Non-Promo Revenue:**
```DAX
Non-Promo Revenue =
CALCULATE (
    SUM (vw_promo_revenue_daily[revenue]), vw_promo_revenue_daily[is_promo] = "f"
)
```

- **Promo Revenue Share:**
```DAX
Promo Revenue Share = DIVIDE ([Promo Revenue], [Promo Revenue] + [Non-Promo Revenue])
```
