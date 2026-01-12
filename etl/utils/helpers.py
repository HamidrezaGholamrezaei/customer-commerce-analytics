# ======================================
# Utility functions for ETL pipeline
# ======================================

import os
import logging
from typing import Any, Dict, Tuple
import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def load_config(path: str) -> Dict[str, Any]:
    """
    Load configuration from the given file path.
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """
    Configure the logger.
    """
    log_cfg = config["logging"]
    logger = logging.getLogger("etl_logger")
    logger.setLevel(getattr(logging, str(log_cfg.get("level", "INFO")).upper()))

    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_cfg.get("log_to_file"):
        log_path = log_cfg["log_file_path"]
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def profile_dataframe(df: pd.DataFrame, logger: logging.Logger, name: str) -> None:
    """
    Log profile information for a dataframe.
    """
    logger.info("--- Profile: %s ---", name)
    logger.info("Shape: %s", df.shape)
    for col in df.columns:
        nulls = df[col].isna().sum()
        dtype = df[col].dtype
        logger.info(" - %s: %s, nulls: %d.", col, dtype, nulls)

def build_date_dimension(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Build a date dimension between the min and max dates in the date column of the input data.
    """
    col = config["data_source"]["date_column"]
    series = df[col].dropna()

    if series.empty:
        raise ValueError("No valid dates found in column %r to build date dimension." % col)

    min_date = series.min().date()
    max_date = series.max().date()
    dates = pd.date_range(start=min_date, end=max_date, freq="D")
    date_dim = pd.DataFrame({
        "full_date": dates,
        "year": dates.year,
        "quarter": dates.quarter,
        "month": dates.month,
        "day": dates.day,
        "is_weekend": dates.weekday >= 5,
    })
    return date_dim

def load_dimension_table(df: pd.DataFrame, config: Dict[str, Any], dim: str, logger: logging.Logger) -> pd.DataFrame:
    """
    Build a dimension table for "item" or "buyer" from the input data.
    """
    cmap = config["column_map"]

    if dim == "item":
        cols = ["item_code", "item_id", "item_name", "category", "version"]
        item_df = df[[cmap[c] for c in cols]].copy()
        item_df.columns = cols

        item_df["item_id"] = pd.to_numeric(item_df["item_id"], errors="coerce").astype("Int64")
        if config["pipeline"]["canonicalize"].get("category"):
            item_df["category"] = (item_df["category"]
                .astype("string")
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
                .str.title()
            )

        strat = config["pipeline"]["canonicalize"]["item_attributes"]
        if strat == "mode":
            def canon(series: pd.Series):
                mode = series.mode()
                return mode.iloc[0] if not mode.empty else series.iloc[0]

            grouped = (item_df.groupby("item_code").agg({
                "item_id": "first",
                "item_name": canon,
                "category": canon,
                "version": canon,
            }).reset_index())

            before = len(grouped)
            grouped = grouped.dropna(subset=["item_id"])
            dropped = before - len(grouped)
            if dropped:
                logger.warning("Dropped %d item rows with null item_id after canonicalization.", dropped)
            return grouped

        item_df = item_df.dropna(subset=["item_id"]).drop_duplicates()
        return item_df

    elif dim == "buyer":
        buyers = df[[cmap["buyer_id"]]].drop_duplicates()
        buyers.columns = ["buyer_id"]
        buyers["buyer_id"] = pd.to_numeric(buyers["buyer_id"], errors="coerce").astype("Int64")

        before = len(buyers)
        buyers = buyers.dropna(subset=["buyer_id"])
        dropped = before - len(buyers)
        if dropped:
            logger.warning("Dropped %d buyer rows with null buyer_id.", dropped)
        return buyers

    else:
        raise ValueError("Unsupported dimension: must be 'item' or 'buyer'")

def load_fact_sales(df: pd.DataFrame, conn: Any, config: Dict[str, Any], logger: logging.Logger) -> None:
    """
    Build and load the fact_sales table.
    """
    cmap = config["column_map"]
    schema = config["postgres"]["schema"]
    chunk_size = config["pipeline"]["chunk_size"]

    # Load dimension tables with keys
    dim_date = pd.read_sql(f"SELECT * FROM {schema}.dim_date", conn)
    dim_item = pd.read_sql(f"SELECT * FROM {schema}.dim_item", conn)
    dim_buyer = pd.read_sql(f"SELECT * FROM {schema}.dim_buyer", conn)
    dim_date["full_date"] = pd.to_datetime(dim_date["full_date"], errors="raise")
    
    # Join to get surrogate keys
    df = df.merge(dim_date[["full_date", "date_key"]], how="left", left_on=cmap["date"], right_on="full_date")
    df = df.merge(dim_item[["item_code", "item_key"]], how="left", left_on=cmap["item_code"], right_on="item_code")
    df = df.merge(dim_buyer[["buyer_id", "buyer_key"]], how="left", left_on=cmap["buyer_id"], right_on="buyer_id")

    missing = df[df[["date_key", "item_key", "buyer_key"]].isnull().any(axis=1)]
    if not missing.empty:
        logger.warning("%d rows dropped due to missing FK references.", missing.shape[0])

    fact = pd.DataFrame({
        "date_key": df["date_key"],
        "item_key": df["item_key"],
        "buyer_key": df["buyer_key"],
        "transaction_id": pd.to_numeric(df[cmap["transaction_id"]], errors="coerce").astype("Int64"),
        "final_quantity": df[cmap["final_quantity"]].astype("Int64"),
        "total_revenue": df[cmap["total_revenue"]].astype(float),
        "price_reductions": df[cmap["price_reductions"]].astype(float),
        "refunds": df[cmap["refunds"]].astype(float),
        "final_revenue": df[cmap["final_revenue"]].astype(float),
        "sales_tax": df[cmap["sales_tax"]].astype(float),
        "overall_revenue": df[cmap["overall_revenue"]].astype(float),
        "refunded_item_count": df[cmap["refunded_item_count"]].astype("Int64"),
        "purchased_item_count": df[cmap["purchased_item_count"]].astype("Int64")
    }).dropna(subset=["date_key", "item_key", "buyer_key", "transaction_id"])

    logger.info("Final fact_sales row count: %d.", fact.shape[0])
    fact.to_sql("fact_sales", conn, schema=schema, if_exists="append", index=False, method="multi", chunksize=chunk_size)
    logger.info("Loaded fact_sales into database.")

def apply_validations(df: pd.DataFrame, config: Dict[str, Any], logger: logging.Logger) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply validation rules defined in the config to data.

    Rules:
    - revenue_balance: final_revenue ≈ total_revenue + price_reductions + refunds
    - overall_balance: overall_revenue ≈ final_revenue + sales_tax
    - quantity_balance: final_quantity = purchased_item_count + refunded_item_count
    - refunded_nonpositive: refunded_item_count <= 0
    """
    cmap = config["column_map"]
    checks = config["validation"]["checks"]
    tolerance = config["validation"]["tolerance"]
    df_original = df.copy()
    start_rows = len(df)

    if checks.get("revenue_balance"):
        df["chk_rev1"] = df[cmap["total_revenue"]] + df[cmap["price_reductions"]] + df[cmap["refunds"]]
        df = df[(df[cmap["final_revenue"]] - df["chk_rev1"]).abs() <= tolerance].copy()


    if checks.get("overall_balance"):
        df["chk_rev2"] = df[cmap["final_revenue"]] + df[cmap["sales_tax"]]
        df = df[(df[cmap["overall_revenue"]] - df["chk_rev2"]).abs() <= tolerance].copy()

    if checks.get("quantity_balance"):
        df["chk_qty"] = df[cmap["purchased_item_count"]] + df[cmap["refunded_item_count"]]
        df = df[df[cmap["final_quantity"]] == df["chk_qty"]].copy()

    if checks.get("refunded_nonpositive"):
        df = df[df[cmap["refunded_item_count"]] <= 0].copy()

    end_rows = len(df)
    dropped = start_rows - end_rows
    drop_rate = dropped / max(start_rows, 1)
    logger.info("Post-validation row count: %d (dropped %d, %.2f%%).", end_rows, dropped, drop_rate * 100.0)

    max_err = config["validation"].get("max_error_rate", 1.0)
    if drop_rate > max_err:
        raise ValueError("Validation drop rate %.2f%% exceeds max_error_rate %.2f%%"
            % (drop_rate * 100.0, max_err * 100.0))

    df_cleaned = df.drop(columns=[c for c in df.columns if c.startswith("chk_")])
    rejected = df_original.loc[~df_original.index.isin(df_cleaned.index)]
    return df_cleaned, rejected

def connect_postgres(pg_cfg: Dict[str, Any]) -> Engine:
    """
    Create a SQLAlchemy engine for PostgreSQL.

    Password resolution:
    - First, try DB_PASSWORD environment variable. 
    - If env var is not set, uses pg_cfg["password"].
    """
    user = pg_cfg["user"]
    password = os.getenv("DB_PASSWORD", pg_cfg.get("password", ""))
    host = pg_cfg["host"]
    port = pg_cfg["port"]
    db = pg_cfg["database"]
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
