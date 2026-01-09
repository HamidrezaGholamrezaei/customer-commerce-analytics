# Customer Commerce Analytics using PostgreSQL and Power BI
*From raw commerce data to decision-ready dashboards.*

## Overview
This repository contains a **Customer analytics project**, designed to make it easy to answer common sales questions, without writing complex SQL each time. It covers the full analytics workflow, including data cleaning and validation, dimensional modeling, and dashboard development.

## Data
The dataset used in this project comes from Kaggle: [Product Sales and Returns Dataset](<https://www.kaggle.com/datasets/yaminh/product-sales-and-returns-dataset>)    
See `data/README.md` for dataset details and download instructions.

## Data Model
The data is modeled using a simple star schema with:
- a central sales fact table
- date, item, and buyer dimensions

A schema diagram is available in `docs/schema_diagram.png`.

## Technology Stack
- PostgreSQL
- Python
- Power BI

## License
MIT License
