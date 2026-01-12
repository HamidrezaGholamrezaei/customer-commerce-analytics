import logging
from pathlib import Path
import pandas as pd
import pytest

from etl.etl_pipeline import load_raw_data, run_etl

# ------------ load_raw_data ------------
@pytest.fixture
def simple_config(tmp_path):
    csv_path = tmp_path / "raw_orders.csv"
    df = pd.DataFrame(
        {
            "order_date": ["02/11/2025", "03/11/2025"],
            "txn_id": ["#123", "ABC456"],
            "buyer": ["b-1", "b-2"],
            "item": ["i-10", "i-20"],
        }
    )
    df.to_csv(csv_path, index=False, sep=";")

    return {
        "data_source": {
            "csv_path": str(csv_path),
            "delimiter": ";",
            "encoding": "utf-8",
            "date_column": "order_date",
            "date_format": "%d/%m/%Y",
        },
        "column_map": {
            "transaction_id": "txn_id",
            "buyer_id": "buyer",
            "item_id": "item",
        },
        "logging": {
            "level": "INFO",
            "log_to_file": False,
        },
    }

def test_load_raw_data(simple_config, caplog):
    logger = logging.getLogger("test_logger")
    df = load_raw_data(simple_config, logger)

    assert pd.api.types.is_datetime64_any_dtype(df["order_date"])
    assert df["order_date"].min().strftime("%Y-%m-%d") == "2025-11-02"
    assert str(df["txn_id"].dtype) == "Int64"
    assert list(df["txn_id"]) == [123, 456]
    assert list(df["buyer"]) == [1, 2]
    assert list(df["item"]) == [10, 20]

# --------------- run_etl ---------------
def _write_full_config(tmp_path, csv_path):
    cfg_path = tmp_path / "etl_config.yaml"
    cfg_path.write_text(f"""
        project:
            name: "testetl"
            version: "0.1"

        logging:
          level: "INFO"
          log_to_file: false
          log_file_path: "{tmp_path}/logs/etl.log"

        paths:
          processed_dir: "{tmp_path}/processed"

        data_source:
          csv_path: "{csv_path}"
          delimiter: ","
          encoding: "utf-8"
          date_column: "order_date"
          date_format: "%Y-%m-%d"

        column_map:
          date: "order_date"
          item_code: "item_code"
          item_id: "item_id"
          item_name: "item_name"
          category: "category"
          version: "version"
          buyer_id: "buyer_id"
          transaction_id: "transaction_id"
          final_quantity: "final_qty"
          total_revenue: "total_rev"
          price_reductions: "disc"
          refunds: "refunds"
          final_revenue: "final_rev"
          sales_tax: "tax"
          overall_revenue: "overall_rev"
          refunded_item_count: "ref_qty"
          purchased_item_count: "purch_qty"

        validation:
          tolerance: 0.01
          max_error_rate: 1.0
          checks:
            revenue_balance: true
            overall_balance: true
            quantity_balance: true
            refunded_nonpositive: true

        pipeline:
          dry_run: true
          chunk_size: 1000
          canonicalize:
            category: true
            item_attributes: "mode"

        postgres:
          user: "testuser"
          password: "testpass"
          host: "localhost"
          port: 5432
          database: "testdb"
          schema: "testschema"
    """)
    return cfg_path

def test_run_etl_dry_run(tmp_path, caplog, monkeypatch):
    csv_path = tmp_path / "orders.csv"
    df = pd.DataFrame(
        {
            "order_date": ["2025-11-02", "2025-11-03"],
            "item_code": ["A", "B"],
            "item_id": [1, 2],
            "item_name": ["Item A", "Item B"],
            "category": ["cat a", "cat b"],
            "version": ["v1", "v1"],
            "buyer_id": [10, 20],
            "transaction_id": [100, 200],
            "final_qty": [1, 2],
            "total_rev": [100.0, 200.0],
            "disc": [0.0, 0.0],
            "refunds": [0.0, 0.0],
            "final_rev": [100.0, 200.0],
            "tax": [10.0, 20.0],
            "overall_rev": [110.0, 220.0],
            "ref_qty": [0, 0],
            "purch_qty": [1, 2],
        }
    )
    df.to_csv(csv_path, index=False)
    cfg_path = _write_full_config(tmp_path, csv_path)
    monkeypatch.setattr("etl.etl_pipeline.load_dotenv", lambda *a, **k: None)
    run_etl(str(cfg_path))

    processed_dir = tmp_path / "processed"
    assert (processed_dir / "orders_cleaned.csv").exists()
    assert (processed_dir / "dim_date.csv").exists()
    assert (processed_dir / "dim_item.csv").exists()
    assert (processed_dir / "dim_buyer.csv").exists()
    assert (processed_dir / "validated_fact_data.csv").exists()

    rejected_path = processed_dir / "rejected_rows.csv"
    assert not rejected_path.exists()

    assert "DRY RUN complete" in caplog.text

def test_run_etl_non_dry_run(monkeypatch, tmp_path):
    csv_path = tmp_path / "orders.csv"
    df = pd.DataFrame(
        {
            "order_date": ["2025-11-02"],
            "item_code": ["A"],
            "item_id": [1],
            "item_name": ["Item A"],
            "category": ["cat a"],
            "version": ["v1"],
            "buyer_id": [10],
            "transaction_id": [100],
            "final_qty": [1],
            "total_rev": [100.0],
            "disc": [0.0],
            "refunds": [0.0],
            "final_rev": [100.0],
            "tax": [10.0],
            "overall_rev": [110.0],
            "ref_qty": [0],
            "purch_qty": [1],
        }
    )
    df.to_csv(csv_path, index=False)

    cfg_path = _write_full_config(tmp_path, csv_path)
    text = cfg_path.read_text().replace("dry_run: true", "dry_run: false")
    cfg_path.write_text(text)

    class DummyConn:
        def __init__(self):
            self.read_sql_calls = []
            self.to_sql_calls = []

    class DummyEngine:
        def begin(self):
            class _Ctx:
                def __init__(self, outer):
                    self.outer = outer
                def __enter__(self):
                    return self.outer.conn
                def __exit__(self, exc_type, exc, tb):
                    return False
            self.conn = DummyConn()
            return _Ctx(self)

    dummy_engine = DummyEngine()
    monkeypatch.setattr("etl.etl_pipeline.connect_postgres", lambda cfg: dummy_engine)

    def fake_read_sql(query, conn, *a, **k):
        conn.read_sql_calls.append(query)
        if "dim_date" in query:
            return pd.DataFrame(
                {
                    "date_key": [1],
                    "full_date": pd.to_datetime(df["order_date"]),
                }
            )
        if "dim_item" in query:
            return pd.DataFrame(
                {
                    "item_key": [10],
                    "item_code": df["item_code"],
                }
            )
        if "dim_buyer" in query:
            return pd.DataFrame(
                {
                    "buyer_key": [100],
                    "buyer_id": df["buyer_id"],
                }
            )
        return pd.DataFrame()

    monkeypatch.setattr("etl.etl_pipeline.pd.read_sql", fake_read_sql)

    def fake_to_sql(self, name, conn, **k):
        conn.to_sql_calls.append(name)
        
    monkeypatch.setattr("etl.etl_pipeline.pd.DataFrame.to_sql", fake_to_sql)

    run_etl(str(cfg_path))

    conn = dummy_engine.conn
    assert any("dim_date" in q for q in conn.read_sql_calls)
    assert any("dim_item" in q for q in conn.read_sql_calls)
    assert any("dim_buyer" in q for q in conn.read_sql_calls)
    assert any(name in ("dim_date", "dim_item", "dim_buyer", "fact_sales") for name in conn.to_sql_calls)
