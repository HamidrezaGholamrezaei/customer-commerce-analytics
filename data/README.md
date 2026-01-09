# Dataset Overview:

**Name:** Product Sales and Returns, which includes sales and return records typical of a retail environment, with transactional details.   
**Source:** [Kaggle - Product Sales and Returns Dataset](<https://www.kaggle.com/datasets/yaminh/product-sales-and-returns-dataset>)  
**Downloaded:** 06.01.2026  
**License:** Apache 2.0

--

**Rows:** 70,052  
**Columns:** 17  
**Date Range:** 01.11.2018 to 30.04.2019

## How to Use This Data in the Repo
This repository does **not** include the full raw dataset. To use the dataset for loading and modeling:

1. Visit the Kaggle URL above.
2. Download the file `order_dataset.csv`.
3. Place the file under:  
```
data/raw/order_dataset.csv
```

## Key Fields
The dataset includes the following fields:

### Item Details
- `Item ID`, `Item Name`, `Item Code`, `Version`, `Category`

### Customer
- `Buyer ID`

### Sales Data
- `Transaction ID`
- `Date`
- `Final Quantity`, `Purchased Item Count`
- `Total Revenue`, `Final Revenue`, `Overall Revenue`
- `Price Reductions`, `Refunds`, `Refunded Item Count`, `Sales Tax`
