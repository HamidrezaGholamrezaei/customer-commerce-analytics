# Data Dictionary

This document explains the data model for this project: what each table represents, how the keys connect, and how to use the model correctly for analysis without misinterpreting metrics.

---

## Model Overview

The warehouse follows a classic **star schema**:

- One central **fact table:** `c360.fact_sales` (sales & returns measures)
- Three **dimensions:**  
  - `c360.dim_date` (when)  
  - `c360.dim_item` (what)  
  - `c360.dim_buyer` (who)

Schema diagram: `docs/schema_diagram.png`

The semantic model builds on analytical views ("vw_*") for faster analysis.

---

## Tables

### 1. `c360.fact_sales`
This is the core transactional table. Each row represents a transaction record (purchase and / or refund) tied to a single date, buyer, and item.

#### Grain
**One row = one item-level transaction record**  
Note that multi-item orders are normal, meaning a single `transaction_id` can appear on multiple rows.

#### Keys
- **Primary Key:** `fact_key` (surrogate, auto-generated)
- **Foreign Keys:**
  - `date_key` → `c360.dim_date.date_key`
  - `item_key` → `c360.dim_item.item_key`
  - `buyer_key` → `c360.dim_buyer.buyer_key`

#### Identifiers
- `transaction_id`: the business order identifier (not unique in this table)

#### Columns:
- `purchased_item_count`: Number of items purchased on this row. Always **>= 0**.

- `refunded_item_count`: Number of items refunded on this row, stored as a **negative integer** (e.g., if `1` unit was refunded → `refunded_item_count = -1`).  

- `final_quantity`: Net quantity for the row:
  ```
  final_quantity = purchased_item_count + refunded_item_count
  ```
  (e.g., if someone buys `2` and refunds `1` → `final_quantity = 1`).

- `total_revenue`: Gross revenue before discounts and refunds are applied.

- `price_reductions`: Discount amount applied, stored as a **negative value** (e.g., `-20.83`). This field represents revenue given up due to discounting.

- `refunds`: Refund amount issued to the customer, stored as a **negative value** (e.g., `-79.17`).

- `final_revenue`: Net revenue after discounts and refunds (excluding tax):
  ```
  final_revenue = total_revenue + price_reductions + refunds
  ``` 

- `sales_tax`: Tax associated with the row. It can be negative if a refund occurs.

- `overall_revenue`: Net revenue **including** tax:
  ```
  overall_revenue ≈ final_revenue + sales_tax
  ```
  It is validated with tolerance (e.g., 0.01 – 0.02) due to rounding.

---

### 2. `c360.dim_date`
A standard calendar dimension used for consistent time-based grouping and filtering.

#### Grain
**One row per calendar date.**

#### Key
- **Primary Key:** `date_key` (surrogate key)

#### Columns
- `full_date`: actual date (unique)
- `year`: year (e.g., 2019)
- `quarter`: 1 – 4
- `month`: 1 – 12
- `day`: 1 – 31
- `is_weekend`: boolean flag

---

### 3. `c360.dim_item`
Contain product attributes used to analyze sales, discounts, and returns.

#### Grain
**One row per unique item code**

#### Key
- **Primary Key:** `item_key` (surrogate key)

#### Columns
- `item_code`: unique item code
- `item_id`: numeric identifier from the dataset
- `item_name`
- `category`
- `version`

---

### 4. `c360.dim_buyer`
Buyer/customer entity table.

#### Grain
**One row per unique buyer.**

#### Key
- **Primary Key:** `buyer_key` (surrogate key)

#### Columns
- `buyer_id`: buyer identifier from the dataset (unique)

---

## Keys & Relationships

### Core star relationships
All joins are **many-to-one** from the fact to the dimensions:

- `c360.fact_sales.date_key` → `c360.dim_date.date_key` (**many** facts to **one** date)
- `c360.fact_sales.item_key` → `c360.dim_item.item_key` (**many** facts to **one** item)
- `c360.fact_sales.buyer_key` → `c360.dim_buyer.buyer_key` (**many** facts to **one** buyer)

An important point here is that `transaction_id` is not a primary key in the fact table (one transaction can have multiple fact rows).

---

## Business Rules Applied

### Sign Conventions
- `price_reductions` (discounts) are **negative**
- `refunds` are **negative**
- `refunded_item_count` is **negative**
### Promo Definition
- In the view layer, a record is treated as **promo** when `price_reductions <> 0`. So “promo” here means “discounted line items”.

---

## Source

Dataset details: see `data/README.md`     
Dataset: Kaggle [Product Sales and Returns Dataset](<https://www.kaggle.com/datasets/yaminh/product-sales-and-returns-dataset>)     
Date Range: 01.11.2018 to 30.04.2019    
