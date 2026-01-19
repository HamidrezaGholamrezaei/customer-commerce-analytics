## Power BI Dashboards
This folder contains screenshots of three Power BI dashboards built on top of the star schema.

### 1. Executive Overview
This dashboard focuses on short-term business performance. It highlights core KPIs such as net revenue, order volume, average order value, and active buyers, with comparisons to the previous period to make changes immediately visible. A split between promotional and non-promotional revenue is included to help distinguish organic growth from discount-driven effects.

### 2. Customer Insights
This dashboard looks at how customers behave over time and how well the business retains them. It includes customer lifetime value distribution, active vs. churned buyers based on inactivity, and cohort-based retention analysis. The cohort view helps compare customer behavior across acquisition periods instead of relying only on overall averages. A detailed buyer table supports ad-hoc analysis of high-value customers, recency, and purchasing patterns.

### 3. Item Performance
This dashboard focuses on product-level performance. It shows which items generate the most revenue, how return rates vary by category, and how discounting relates to overall product performance.

### Notes
- Data source: PostgreSQL analytics views
- Time window: Nov 2018 – Apr 2019
- Churn definition: 30-day inactivity
