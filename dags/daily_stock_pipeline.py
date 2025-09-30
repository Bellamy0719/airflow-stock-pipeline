from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import great_expectations as ge
from great_expectations.core.batch import Batch
from great_expectations.validator.validator import Validator
from great_expectations.execution_engine import PandasExecutionEngine
import yfinance as yf
import pandas as pd
import os
import logging
from sqlalchemy import create_engine, text

# Raw data path
RAW_DIR = "/opt/airflow/data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

# Airflow manages logging config, use module-level logger
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],  # notification recipient
    "email_on_failure": True,             # notify on task failure
    "email_on_retry": False,              # no email on retry
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def fetch_and_transform_stock_data():

    """
    Extract stock price data via yfinance,
    calculate technical indicators,
    and save as a CSV for downstream tasks.
    """

    tickers = ["AAPL", "MSFT", "AMZN", "GOOGL", "TSLA"]
    LOOKBACK_DAYS = 365  # enough window for SMA200/MACD/RSI
    all_data = []

    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/stocks")
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(date) FROM stocks_features")) # Use latest date in DB to decide incremental vs full load
            max_date_in_db = result.scalar()
            logger.info("Max date in DB: %s", max_date_in_db)
    except Exception as e:
        max_date_in_db = None
        logger.warning("DB query failed, treating as first full run: %s", e)

    for ticker in tickers:
        if max_date_in_db:
            # Look back to ensure indicators have enough history
            start_date = (pd.to_datetime(max_date_in_db) - pd.Timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
            logger.info("Downloading %s data since %s (with lookback window)", ticker, start_date)
            df = yf.download(ticker, start=start_date, interval="1d", auto_adjust=True)
        else:
            # First run: download more history
            logger.info("First run, downloading 2 years of %s data", ticker)
            df = yf.download(ticker, period="2y", interval="1d", auto_adjust=True)

        df.reset_index(inplace=True)
        df.dropna(inplace=True)

        # --- Flatten multi-index columns ---
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

        # ===== Technical Indicators =====
        # Includes SMA, volatility, RSI, MACD, Bollinger Bands, etc.
        df["sma_20"] = df["Close"].rolling(20).mean()
        df["sma_50"] = df["Close"].rolling(50).mean()
        df["sma_200"] = df["Close"].rolling(200).mean()
        df["volatility_20"] = df["Close"].pct_change().rolling(20).std()

        # RSI(14)
        delta = df["Close"].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        rs = gain.rolling(14).mean() / loss.rolling(14).mean()
        df["RSI"] = 100 - (100 / (1 + rs))

        # MACD(12,26,9)
        ema12 = df["Close"].ewm(span=12, adjust=False).mean()
        ema26 = df["Close"].ewm(span=26, adjust=False).mean()
        df["MACD"] = ema12 - ema26
        df["Signal_Line"] = df["MACD"].ewm(span=9, adjust=False).mean()

        # Bollinger Bands (20, 2)
        mid = df["Close"].rolling(20).mean()
        std = df["Close"].rolling(20).std()
        df["Bollinger_Upper"] = mid + 2 * std
        df["Bollinger_Lower"] = mid - 2 * std

        # === Extra indicators ===
        # Volume moving average
        df["vol_ma_20"] = df["Volume"].rolling(20).mean()

        # Buy/Sell flags (simple: MA + RSI)
        # Simple trading signals: SMA crossover + RSI thresholds
        df["buy_flag"] = (df["sma_20"] > df["sma_50"]) & (df["RSI"] < 30)
        df["sell_flag"] = (df["sma_20"] < df["sma_50"]) & (df["RSI"] > 70)

        # Golden/Death cross
        df["golden_cross"] = (df["sma_50"] > df["sma_200"]) & (df["sma_50"].shift(1) <= df["sma_200"].shift(1))
        df["death_cross"] = (df["sma_50"] < df["sma_200"]) & (df["sma_50"].shift(1) >= df["sma_200"].shift(1))

        # --- Normalize column names ---
        df = df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
            "RSI": "rsi",
            "MACD": "macd",
            "Signal_Line": "signal_line",
            "Bollinger_Upper": "bollinger_upper",
            "Bollinger_Lower": "bollinger_lower",
        })
        df["ticker"] = ticker
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # Keep only rows after max_date_in_db
        if max_date_in_db:
            cutoff = pd.to_datetime(max_date_in_db).date()
            df = df[df["date"] > cutoff]

        # Column type enforcement
        num_cols = [
            "open","high","low","close","volume",
            "sma_20","sma_50","sma_200","volatility_20",
            "rsi","macd","signal_line","bollinger_upper","bollinger_lower",
            "vol_ma_20","buy_flag","sell_flag","golden_cross","death_cross"
        ]
        for c in num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # Skip if no new rows (weekend/holiday)
        if df.empty:
            logger.info("%s: no new rows.", ticker)
            continue

        df = df[[
            "ticker","date","open","high","low","close","volume",
            "sma_20","sma_50","sma_200","volatility_20","rsi","macd",
            "signal_line","bollinger_upper","bollinger_lower",
            "vol_ma_20","buy_flag","sell_flag","golden_cross","death_cross"
        ]]
        logger.info("%s: new rows added: %d", ticker, len(df))
        all_data.append(df)

    if not all_data:
        logger.warning("No new data for any ticker this run")
        return None

    final_df = pd.concat(all_data, ignore_index=True) # Concatenate all tickers' data and save to CSV
    file_path = os.path.join(RAW_DIR, f"stocks_{datetime.now().strftime('%Y-%m-%d')}_etl.csv")
    final_df.to_csv(file_path, index=False)
    logger.info("File saved: %s; total new rows: %d", file_path, len(final_df))
    return file_path


def load_to_postgres(ti):

    """
    Load the transformed CSV into PostgreSQL table: stocks_features.
    """

    file_path = ti.xcom_pull(task_ids="fetch_and_transform_stock_data")
    if not file_path:
        logger.info("No file path received from fetch task, skipping DB load.")
        return

    logger.info("Loading CSV: %s", file_path)
    df = pd.read_csv(file_path, parse_dates=["date"])
    df["date"] = pd.to_datetime(df["date"]).dt.date

    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/stocks")
    df.to_sql(
        "stocks_features",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    logger.info("Data inserted into PostgreSQL, rows: %d", len(df))


def validate_data(ti):

    """
    Validate the transformed data before loading into DB.
    Checks: null values, duplicates, invalid prices.
    """
      
    file_path = ti.xcom_pull(task_ids="fetch_and_transform_stock_data")
    if not file_path:
        logger.info("No file path received from fetch task, skipping validation.")
        return

    logger.info("Validating CSV: %s", file_path)
    df = pd.read_csv(file_path)

    # Data validation: nulls, duplicates, price range
    if df["date"].isnull().any():
        raise ValueError("Found NULL in date column")
    if df[["ticker", "date"]].duplicated().any():
        raise ValueError("Found duplicate (ticker, date) rows")
    if (df["close"] <= 0).any():
        raise ValueError("Found invalid close price <= 0")

    logger.info("Data validation passed")


# DAG definition: daily run, no backfill (catchup=False)
with DAG(
    dag_id="daily_stock_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 9, 12),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_etl = PythonOperator(
        task_id="fetch_and_transform_stock_data",
        python_callable=fetch_and_transform_stock_data,
    )

    task_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    task_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    # Task dependency order: ETL -> Validate -> Load
    task_etl >> task_validate >> task_load
