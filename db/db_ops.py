from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine
from db.db_schema import stock_data
import sqlalchemy.exc as sql_exec
from config.logger import logger
from dotenv import load_dotenv
import pandas as pd
import time
import pytz
import os

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DATABASE_USERNAME')}:{os.getenv('DATABASE_PASSWORD')}@{os.getenv('DATABASE_HOSTNAME')}:{os.getenv('DATABASE_PORT')}/{os.getenv('DATABASE_NAME')}"
)


def get_data(table_name="stock_data"):
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, engine.connect())

    # Detect and convert all datetime columns to IST
    ist = pytz.timezone("Asia/Kolkata")
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # Assume UTC if timezone-naive, then convert to IST
            df[col] = (
                pd.to_datetime(df[col], utc=True, errors="coerce")
                .dt.tz_convert(ist)
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )

    # Save to CSV
    df.to_csv('output_csv.csv', index=False, encoding="utf-8-sig")


def insert_stock_data(table, data_dict, retry=3, wait_period=15):
    stmt = insert(stock_data)
    update_cols = {
        c: stmt.excluded[c] for c in [
            'period_start', 'period_end',
            'fiscal_type', 'metric_value', 'updated_at'
        ]
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=['symbol', 'timestamp', 'period_code', 'metric_name'],
        set_=update_cols
    )
    for attempt in range(retry + 1):
        try:
            with engine.connect() as con:
                con.execute(stmt, data_dict)
                con.commit()
                logger.info("Inserted successfully")
                return
        except sql_exec.OperationalError as e:
            logger.error(f"Retrying {attempt + 1}/{retry} again OperationalError on {table.name}: {e}")
        except Exception as e:
            logger.error(f"Retrying {attempt + 1}/{retry} again general DB error for {table.name}: {e}")
        if attempt < retry:
            logger.warning(f"Retrying in {wait_period} seconds..")
            time.sleep(wait_period)
        else:
            logger.error(f"Final failure inserting into {table.name}. Skipping.")

    return None


if __name__ == '__main__':
    get_data()
