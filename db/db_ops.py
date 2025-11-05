from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine
from db.db_schema import stock_data
import sqlalchemy.exc as sql_exec
from config.logger import logger
from dotenv import load_dotenv
import time
import os

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DATABASE_USERNAME')}:{os.getenv('DATABASE_PASSWORD')}@{os.getenv('DATABASE_HOSTNAME')}:{os.getenv('DATABASE_PORT')}/{os.getenv('DATABASE_NAME')}"
)


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
