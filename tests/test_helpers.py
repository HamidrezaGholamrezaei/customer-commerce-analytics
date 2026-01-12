import pandas as pd
import pytest

from etl.utils.helpers import (
    setup_logging,
    build_date_dimension,
    load_dimension_table,
    apply_validations,
    load_fact_sales,
    connect_postgres,
)


# --------- build_date_dimension --------
def _minimal_config_with_date_col(col_name):
    return {
        "data_source": {
            "date_column": col_name
        }
    }

def test_build_date_dimension_normal_case():
    df = pd.DataFrame({
        "order_date": pd.to_datetime([
            "2025-11-02",
            "2025-11-10",
        ])
    })
    cfg = _minimal_config_with_date_col("order_date")
    dim = build_date_dimension(df, cfg)

    assert len(dim) == 9
    assert dim["full_date"].min().date().isoformat() == "2025-11-02"
    assert dim["full_date"].max().date().isoformat() == "2025-11-10"
    assert dim["is_weekend"].dtype == "bool"
    for col in ["year", "quarter", "month", "day", "is_weekend"]:
        assert col in dim.columns

def test_build_date_dimension_no_valid_dates():
    df = pd.DataFrame({"date": [pd.NaT, pd.NaT]})
    cfg = _minimal_config_with_date_col("date")

    with pytest.raises(ValueError):
        build_date_dimension(df, cfg)

# --------- load_dimension_table --------
@pytest.fixture
def column_map():
    return {
        "item_code": "raw_item_code",
        "item_id": "raw_item_id",
        "item_name": "raw_item_name",
        "category": "raw_category",
        "version": "raw_version",
        "buyer_id": "raw_buyer_id",
    }

@pytest.fixture
def pipeline_config():
    return {
        "pipeline": {
            "canonicalize": {
                "category": True,
                "item_attributes": "mode",
            }
        }
    }

def test_load_dimension_table_with_mode_canonicalization(column_map, pipeline_config, caplog):
    cfg = {"column_map": column_map, **pipeline_config}
    df = pd.DataFrame(
        {
            "raw_item_code": ["A", "A", "B"],
            "raw_item_id": ["1", "1", "2"],
            "raw_item_name": ["item A", "ITEM A", "item B"],
            "raw_category": [" electronics ", "Electronics", "home APPLIANCES"],
            "raw_version": ["v1", "v1", "v2"],
        }
    )

    logger = setup_logging({"logging": {"level": "INFO", "log_to_file": False}})
    dim_item = load_dimension_table(df, cfg, dim="item", logger=logger)

    assert set(dim_item["item_code"]) == {"A", "B"}
    assert dim_item.loc[dim_item["item_code"] == "A", "item_id"].iloc[0] == 1
    assert set(dim_item["category"]) == {"Electronics", "Home Appliances"}

def test_load_dimension_table_drops_nulls_and_logs_warning(column_map, caplog):
    cfg = {"column_map": column_map, "pipeline": {}}
    df = pd.DataFrame(
        {"raw_buyer_id": ["123", "x", None]}
    )

    logger = setup_logging({"logging": {"level": "INFO", "log_to_file": False}})

    with caplog.at_level("WARNING"):
        buyers = load_dimension_table(df, cfg, dim="buyer", logger=logger)

    assert list(buyers["buyer_id"]) == [123]
    assert any("Dropped" in rec.message for rec in caplog.records)

def test_load_dimension_table_unsupported_dim_raises(column_map):
    cfg = {"column_map": column_map, "pipeline": {}}
    df = pd.DataFrame()
    logger = setup_logging({"logging": {"level": "INFO", "log_to_file": False}})

    with pytest.raises(ValueError):
        load_dimension_table(df, cfg, dim="unknown", logger=logger)

# ---------- apply_validations ----------
@pytest.fixture
def validation_config(column_map):
    return {
        "column_map": {
            **column_map,
            "date": "order_date",
            "transaction_id": "txn_id",
            "total_revenue": "total",
            "price_reductions": "discount",
            "refunds": "refunds",
            "final_revenue": "final_rev",
            "sales_tax": "tax",
            "overall_revenue": "overall",
            "final_quantity": "final_qty",
            "refunded_item_count": "refund_qty",
            "purchased_item_count": "purch_qty",
        },
        "validation": {
            "tolerance": 0.01,
            "max_error_rate": 0.5,
            "checks": {
                "revenue_balance": True,
                "overall_balance": True,
                "quantity_balance": True,
                "refunded_nonpositive": True,
            },
            
        },
    }

def test_apply_validations_normal_case(validation_config):
    df = pd.DataFrame(
        {
            "total": [100.0, 100.0],
            "discount": [-10.0, -10.0],
            "refunds": [0.0, 5.0],
            "final_rev": [90.0, 95.0],
            "tax": [9.0, 9.0],
            "overall": [99.0, 104.0],
            "final_qty": [10, 9],
            "purch_qty": [10, 10],
            "refund_qty": [0, 1],
            "raw_item_code": ["A", "B"],
            "raw_item_id": [1, 2],
            "raw_item_name": ["A", "B"],
            "raw_category": ["cat", "cat"],
            "raw_version": ["v1", "v1"],
            "raw_buyer_id": [1, 2],
        }
    )

    logger = setup_logging({"logging": {"level": "INFO", "log_to_file": False}})

    cleaned, rejected = apply_validations(df, validation_config, logger)

    assert len(cleaned) == 1
    assert len(rejected) == 1
    assert cleaned.iloc[0]["total"] == 100.0

def test_apply_validations_drop_rate_exceeds_max(validation_config):
    df = pd.DataFrame(
        {
            "total": [100.0, 200.0],
            "discount": [0.0, 0.0],
            "refunds": [0.0, 0.0],
            "final_rev": [100.0, 200.0],
            "tax": [10.0, 20.0],
            "overall": [110.0, 220.0],
            "final_qty": [1, 1],
            "purch_qty": [0, 0],
            "refund_qty": [0, 0],
            "raw_item_code": ["A", "B"],
            "raw_item_id": [1, 2],
            "raw_item_name": ["A", "B"],
            "raw_category": ["cat", "cat"],
            "raw_version": ["v1", "v1"],
            "raw_buyer_id": [1, 2],
        }
    )

    validation_config["validation"]["max_error_rate"] = 0.1
    logger = setup_logging({"logging": {"level": "INFO", "log_to_file": False}})
    with pytest.raises(ValueError):
        apply_validations(df, validation_config, logger)

# ----------- load_fact_sales -----------
def test_load_fact_sales(monkeypatch, validation_config, caplog):
    cmap = validation_config["column_map"]
    schema = "testschema"
    cfg = {
        **validation_config,
        "column_map": cmap,
        "postgres": {"schema": schema},
        "pipeline": {"chunk_size": 100},
    }

    # Fact DF
    df = pd.DataFrame(
        {
            cmap["date"]: pd.to_datetime(["2025-11-02", "2025-11-03"]),
            cmap["item_code"]: ["A", "B"],
            cmap["buyer_id"]: [1, 2],
            cmap["transaction_id"]: ["10", "20"],
            cmap["final_quantity"]: [1, 2],
            cmap["total_revenue"]: [100.0, 200.0],
            cmap["price_reductions"]: [0.0, 0.0],
            cmap["refunds"]: [0.0, 0.0],
            cmap["final_revenue"]: [100.0, 200.0],
            cmap["sales_tax"]: [10.0, 20.0],
            cmap["overall_revenue"]: [110.0, 220.0],
            cmap["refunded_item_count"]: [0, 0],
            cmap["purchased_item_count"]: [1, 2],
        }
    )

     # Fake dim tables
    dim_date = pd.DataFrame(
        {
            "date_key": [1, 2],
            "full_date": pd.to_datetime(["2025-11-02", "2025-11-03"]),
        }
    )
    dim_item = pd.DataFrame({"item_key": [10, 20], "item_code": ["A", "B"]})
    dim_buyer = pd.DataFrame({"buyer_key": [100, 200], "buyer_id": [1, 2]})

    read_calls = []

    def fake_read_sql(query, conn, *args, **kwargs):
        read_calls.append(query)
        if "dim_date" in query:
            return dim_date
        if "dim_item" in query:
            return dim_item
        if "dim_buyer" in query:
            return dim_buyer
        raise AssertionError("Unexpected query: " + query)

    monkeypatch.setattr("etl.utils.helpers.pd.read_sql", fake_read_sql)

    written = {}

    def fake_to_sql(self, name, conn, schema=None, if_exists=None, index=None, method=None, chunksize=None):
        written["name"] = name
        written["schema"] = schema
        written["df"] = self.copy()

    monkeypatch.setattr("etl.utils.helpers.pd.DataFrame.to_sql", fake_to_sql)

    conn = object()
    logger = setup_logging({"logging": {"level": "INFO", "log_to_file": False}})

    with caplog.at_level("INFO"):
        load_fact_sales(df, conn, cfg, logger)

    assert any("dim_date" in q for q in read_calls)
    assert any("dim_item" in q for q in read_calls)
    assert any("dim_buyer" in q for q in read_calls)

    assert written["name"] == "fact_sales"
    assert written["schema"] == schema
    fact_df = written["df"]
    assert list(fact_df.columns)[:4] == ["date_key", "item_key", "buyer_key", "transaction_id"]
    assert len(fact_df) == 2

# ----------- connect_postgres ----------
def test_connect_postgres(monkeypatch):
    cfg = {
        "user": "testuser",
        "password": "from_cfg",
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
    }
    monkeypatch.setenv("DB_PASSWORD", "from_env")

    engine = connect_postgres(cfg)
    url = engine.url

    assert url.username == "testuser"
    assert url.database == "testdb"
    assert url.password == "from_env"
