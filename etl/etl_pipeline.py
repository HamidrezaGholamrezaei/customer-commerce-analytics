# ========================
# ETL Pipeline
# ========================

import logging
from pathlib import Path
from typing import Any, Dict
import pandas as pd
from dotenv import load_dotenv

from .utils.helpers import (
    load_config,
    setup_logging,
    profile_dataframe,
    build_date_dimension,
    apply_validations,
    connect_postgres,
    load_dimension_table,
    load_fact_sales,
)

def load_raw_data(config: Dict[str, Any], logger: logging.Logger) -> pd.DataFrame:
    """
    Load the raw CSV and perform cleaning.
    """
    csv_path = Path(config["data_source"]["csv_path"])
    logger.info("Loading raw data from: %s.", csv_path)

    cmap = config["column_map"]
    ds_cfg = config["data_source"]
    id_dtype = {
        cmap["transaction_id"]: "string",
        cmap["buyer_id"]: "string",
        cmap["item_id"]: "string",
    }

    df = pd.read_csv(
        csv_path,
        delimiter=ds_cfg["delimiter"],
        encoding=ds_cfg["encoding"],
        dtype=id_dtype,
    )

    date_col = ds_cfg["date_column"]
    fmt = ds_cfg["date_format"]
    df[date_col] = pd.to_datetime(df[date_col], format=fmt, errors="coerce")

    # Clean ID fields
    for col in (cmap["transaction_id"], cmap["buyer_id"], cmap["item_id"]):
        df[col] = (df[col]
            .astype("string")
            .str.replace(r"[^0-9]", "", regex=True)
            .replace({"": pd.NA})
        )
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    logger.info("Loaded dataset: %d rows.", df.shape[0])
    profile_dataframe(df, logger, name="Raw Input")
    return df

def run_etl(config_path: str = "etl/etl_config.yaml") -> None:
    """
    Run the ETL process: load config, load and validate raw data,
    and build dimensions and fact table into Postgres.
    """
    load_dotenv()
    config = load_config(config_path)
    logger = setup_logging(config)

    proj_name = config["project"]["name"]
    proj_ver = config["project"]["version"] 
    logger.info("Starting ETL for '%s' (version=%s).", proj_name, proj_ver)

    processed_dir = Path(config["paths"]["processed_dir"])
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    df_raw = load_raw_data(config, logger)
    cleaned_path = processed_dir / "orders_cleaned.csv"
    df_raw.to_csv(cleaned_path, index=False)
    logger.info("Saved cleaned data to %s.", cleaned_path)

    # Build dimension tables
    logger.info("Generating dimensions...")

    dim_date = build_date_dimension(df_raw, config)
    dim_date_path = processed_dir / "dim_date.csv"
    dim_date.to_csv(dim_date_path, index=False)

    dim_item = load_dimension_table(df_raw, config, dim="item", logger=logger)
    dim_item_path = processed_dir / "dim_item.csv"
    dim_item.to_csv(dim_item_path, index=False)

    dim_buyer = load_dimension_table(df_raw, config, dim="buyer", logger=logger)
    dim_buyer_path = processed_dir / "dim_buyer.csv"
    dim_buyer.to_csv(dim_buyer_path, index=False)

    logger.info("Saved dimension tables to %s.", processed_dir)

    logger.info("Validating fact data...")
    df_validated, rejected = apply_validations(df_raw.copy(), config, logger)
    validated_path = processed_dir / "validated_fact_data.csv"
    df_validated.to_csv(validated_path, index=False)
    logger.info("Saved validated rows to %s.", validated_path)

    if not rejected.empty:
        rejected_path = processed_dir / "rejected_rows.csv"
        rejected.to_csv(rejected_path, index=False)
        logger.info("Saved rejected rows to %s.", rejected_path)
    else:
        logger.info("No rejected rows from validation.")

    dry_run = config["pipeline"].get("dry_run", False)
    if dry_run:
        logger.info("DRY RUN complete:")
        logger.info("  - dim_date rows prepared: %d.", len(dim_date))
        logger.info("  - dim_item rows prepared: %d.", len(dim_item))
        logger.info("  - dim_buyer rows prepared: %d.", len(dim_buyer))
        logger.info("  - fact rows validated: %d.", len(df_validated))
        logger.info("No data was written to the database because dry_run=True.")
        return
    
    # Load to PostgreSQL
    engine = connect_postgres(config["postgres"])
    chunk_size = config["pipeline"]["chunk_size"]
    schema = config["postgres"]["schema"]
    to_sql_kwargs = dict(
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=chunk_size,
    )

    logger.info("Loading data into PostgreSQL...")
    with engine.begin() as conn:
        # dim_date
        existing_dates = pd.read_sql(f"SELECT full_date FROM {schema}.dim_date", conn)
        if not existing_dates.empty:
            existing_dates["full_date"] = pd.to_datetime(existing_dates["full_date"], errors="raise")
        new_dates = (
            dim_date.merge(existing_dates, on="full_date", how="left", indicator=True)
            .query("_merge == 'left_only'")
            .drop(columns=["_merge"])
        )
        if not new_dates.empty:
            logger.info("Inserting %d new dim_date rows.", len(new_dates))
            new_dates.to_sql("dim_date", conn, **to_sql_kwargs)
        else:
            logger.info("No new dim_date rows to insert.")

        # dim_item
        existing_items = pd.read_sql(f"SELECT item_code FROM {schema}.dim_item", conn)
        new_items = (
            dim_item.merge(existing_items, on="item_code", how="left", indicator=True)
            .query("_merge == 'left_only'")
            .drop(columns=["_merge"])
        )
        if not new_items.empty:
            logger.info("Inserting %d new dim_item rows.", len(new_items))
            new_items.to_sql("dim_item", conn, **to_sql_kwargs)
        else:
            logger.info("No new dim_item rows to insert.")

        # dim_buyer
        existing_buyers = pd.read_sql(f"SELECT buyer_id FROM {schema}.dim_buyer", conn)
        new_buyers = (
            dim_buyer.merge(existing_buyers, on="buyer_id", how="left", indicator=True)
            .query("_merge == 'left_only'")
            .drop(columns=["_merge"])
        )
        if not new_buyers.empty:
            logger.info("Inserting %d new dim_buyer rows.", len(new_buyers))
            new_buyers.to_sql("dim_buyer", conn, **to_sql_kwargs)
        else:
            logger.info("No new dim_buyer rows to insert.")

        # fact_sales
        load_fact_sales(df_validated, conn, config, logger)

    logger.info("ETL process completed successfully.")

if __name__ == "__main__":
    run_etl()
