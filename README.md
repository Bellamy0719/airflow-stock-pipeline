# Automated-Production-Grade-Stock-Data-Pipeline-Airflow-Docker-PostgreSQL-Tableau
Automated stock data pipeline using Airflow, Docker, PostgreSQL, and Tableau. Fetches daily market data, computes technical indicators (SMA, RSI, MACD, Bollinger Bands), validates data quality, performs incremental updates, and stores results for analytics. Near production-grade with room for future improvements.

Tech Stack
- **Orchestration**: Apache Airflow
- **Data Source**: yfinance
- **Database**: PostgreSQL (via SQLAlchemy)
- **ETL**: Pandas + custom feature engineering
- **Validation**: Data quality checks (Great Expectations prototype)
- **Visualization**: Tableau
- **CI/CD**: GitHub Actions (pytest)

Features
- Automated daily stock ETL (AAPL, MSFT, AMZN, GOOGL, TSLA)
- Incremental updates (no duplicate inserts)
- Technical indicators: SMA, RSI, MACD, Bollinger Bands, Volatility
- Buy/Sell signals (Golden Cross, Death Cross)
- Logging with Airflow-friendly format
- Data quality checks (nulls, duplicates, invalid prices)

 Project Structure
```bash
airflow-stock-pipeline/
├── dags/                  # Airflow DAGs
├── docker/                # Docker configs
├── config/                # Variables and env templates
├── tests/                 # Unit tests (pytest)
├── notebooks/             # Exploratory notebooks / Tableau demo
├── .github/workflows/     # CI/CD workflows
```

flowchart TD
    A[Fetch stock data] --> B[Transform & add indicators]
    B --> C[Validate data quality]
    C --> D[Load into PostgreSQL]
    D --> E[Visualize in Tableau]

Visualization Example

(Insert Tableau screenshot here — e.g., SMA + Buy/Sell signals on AAPL)

Future Improvements

-Parameterization: Move tickers & configs to Airflow Variables / .env
-Monitoring & Alerts: Prometheus + Grafana, Slack/email alerts
-More indicators: Sharpe Ratio, ATR, Beta, Volume MAs
-Trading signals: More advanced buy/sell logic
-Prediction models: ARIMA, Random Forest, simple ML models
-Data governance: Great Expectations reports, schema versioning
-Engineering practices: CI/CD with Docker builds, unit tests, black/flake8 auto-formatting

How to Run
1. Copy the repo
   git clone https://github.com/Bellamy0719/airflow-stock-pipeline.git
   cd airflow-stock-pipeline
2. Start Services
   docker-compose up -d
3. Open Airflow UI at https://localhost:8080
