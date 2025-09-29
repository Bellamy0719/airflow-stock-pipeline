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

# 保存路径
RAW_DIR = "/opt/airflow/data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

# Airflow 会接管 logging 配置，这里只取一个模块级 logger
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],  # 收件人
    "email_on_failure": True,             # 任务失败时发邮件
    "email_on_retry": False,              # 重试时是否发邮件
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def fetch_and_transform_stock_data():
    tickers = ["AAPL", "MSFT", "AMZN", "GOOGL", "TSLA"]
    LOOKBACK_DAYS = 365  # 覆蓋到 SMA200/MACD/RSI 的需求
    all_data = []

    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/stocks")
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(date) FROM stocks_features"))
            max_date_in_db = result.scalar()
            logger.info("DB 中最大日期: %s", max_date_in_db)
    except Exception as e:
        max_date_in_db = None
        logger.warning("查询 DB 失败，视为首次全量运行：%s", e)

    for ticker in tickers:
        if max_date_in_db:
            # ⭐ 回看一段歷史，確保有足夠窗口
            start_date = (pd.to_datetime(max_date_in_db) - pd.Timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
            logger.info("开始下载 %s 自 %s 的数据（含回看窗口）", ticker, start_date)
            df = yf.download(ticker, start=start_date, interval="1d", auto_adjust=True)
        else:
            # 首次全量，多給一些歷史
            logger.info("首次运行，下载 %s 过去 2 年数据", ticker)
            df = yf.download(ticker, period="2y", interval="1d", auto_adjust=True)

        df.reset_index(inplace=True)
        df.dropna(inplace=True)

        # --- 扁平化列 ---
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

        # ===== 計算指標 =====
        df["sma_20"] = df["Close"].rolling(20).mean()
        df["sma_50"] = df["Close"].rolling(50).mean()
        df["sma_200"] = df["Close"].rolling(200).mean()
        df["volatility_20"] = df["Close"].pct_change().rolling(20).std()  # ← 保留原名

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

        # Bollinger(20, 2)
        mid = df["Close"].rolling(20).mean()
        std = df["Close"].rolling(20).std()
        df["Bollinger_Upper"] = mid + 2 * std
        df["Bollinger_Lower"] = mid - 2 * std

        # === 新增指標 ===
        # 成交量均线
        df["vol_ma_20"] = df["Volume"].rolling(20).mean()

        # Buy/Sell flags (簡單版: 均線+RSI)
        df["buy_flag"] = (df["sma_20"] > df["sma_50"]) & (df["RSI"] < 30)
        df["sell_flag"] = (df["sma_20"] < df["sma_50"]) & (df["RSI"] > 70)

        # 黄金交叉 / 死亡交叉
        df["golden_cross"] = (df["sma_50"] > df["sma_200"]) & (df["sma_50"].shift(1) <= df["sma_200"].shift(1))
        df["death_cross"] = (df["sma_50"] < df["sma_200"]) & (df["sma_50"].shift(1) >= df["sma_200"].shift(1))

        # --- 標準化欄名 ---
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
            "vol_ma_20": "vol_ma_20",
            "buy_flag": "buy_flag",
            "sell_flag": "sell_flag",
            "golden_cross": "golden_cross",
            "death_cross": "death_cross",
        })
        df["ticker"] = ticker
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # ⭐ 指標已用回看資料算好，入庫時只留真正新日期
        if max_date_in_db:
            cutoff = pd.to_datetime(max_date_in_db).date()
            df = df[df["date"] > cutoff]

        # 欄位順序 & 型別
        num_cols = [
            "open","high","low","close","volume",
            "sma_20","sma_50","sma_200","volatility_20",
            "rsi","macd","signal_line","bollinger_upper","bollinger_lower",
            "vol_ma_20","buy_flag","sell_flag","golden_cross","death_cross"
        ]
        for c in num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # 若這檔沒有新資料（週末/假期），略過
        if df.empty:
            logger.info("%s: no new rows.", ticker)
            continue

        df = df[[
            "ticker","date","open","high","low","close","volume",
            "sma_20","sma_50","sma_200","volatility_20","rsi","macd",
            "signal_line","bollinger_upper","bollinger_lower",
            "vol_ma_20","buy_flag","sell_flag","golden_cross","death_cross"
        ]]
        logger.info("%s: 新增行数 %d", ticker, len(df))
        all_data.append(df)

    if not all_data:
        logger.warning("所有 tickers 本次均无新增数据")
        return None

    final_df = pd.concat(all_data, ignore_index=True)
    file_path = os.path.join(RAW_DIR, f"stocks_{datetime.now().strftime('%Y-%m-%d')}_etl.csv")
    final_df.to_csv(file_path, index=False)
    logger.info("保存成功：%s；新增 %d 行", file_path, len(final_df))
    return file_path


def load_to_postgres(ti):
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
    file_path = ti.xcom_pull(task_ids="fetch_and_transform_stock_data")
    if not file_path:
        logger.info("No file path received from fetch task, skipping validation.")
        return

    logger.info("Validating CSV: %s", file_path)
    df = pd.read_csv(file_path)

    # 数据验证：可逐步扩展（空值/重复/价格合理性/日期范围等）
    if df["date"].isnull().any():
        raise ValueError("Found NULL in date column")
    if df[["ticker", "date"]].duplicated().any():
        raise ValueError("Found duplicate (ticker, date) rows")
    if (df["close"] <= 0).any():
        raise ValueError("Found invalid close price <= 0")

    logger.info("Data validation passed")


# DAG 定義
with DAG(
    dag_id="daily_stock_pipeline_v20250928",
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

    # 依赖关系
    task_etl >> task_validate >> task_load
