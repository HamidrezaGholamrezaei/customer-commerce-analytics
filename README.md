# Customer Commerce Analytics using PostgreSQL and Power BI
*From raw commerce data to decision-ready dashboards.*

This repository contains a small **Customer analytics project**, designed to make it easy to answer common sales questions. It covers the full analytics workflow, including data cleaning and validation, dimensional modeling, and visualize the results in Power BI report pages (screenshots in `docs/`).

---

## Data Source
The dataset used in this project comes from Kaggle: [Product Sales and Returns Dataset](<https://www.kaggle.com/datasets/yaminh/product-sales-and-returns-dataset>)    
See `data/README.md` for dataset details.

## Data Model
The data is modeled using a simple star schema with:
- a central sales fact table
- date, item, and buyer dimensions

A schema diagram is available in `docs/schema_diagram.png`.

---

## What this project answers
**Sales performance**
- Net revenue and orders over time
- 7-day rolling trends and comparisons to the previous period
- Promo vs non-promo performance (using discount as a promo flag)

**Customer**
- Who is active vs churned (30-day inactivity)
- Customer lifetime value (CLV) and average order value (AOV)
- Retention by acquisition cohort

**Items**
- Top items by revenue
- Return rate by item and by category
- Discount share and revenue ranking within each category

---

## How to run
1. Download `order_dataset.csv` and place it under `data/raw/` (see `data/README.md`).
2. Create the database and schema (SQL scripts in `sql/`).
3. Run the ETL (`etl/`) to load/clean data and populate the warehouse tables.
4. Create analytics views (SQL scripts in `sql/`).
5. Use the views as the source for Power BI.

---

## Technology Stack
- PostgreSQL
- Python
- Power BI

---

## Contributing
Contributions are welcome! Please open an issue or submit a pull request.

---

## License
This project is licensed under the MIT License. For more detailed information, please refer to the LICENSE file.
