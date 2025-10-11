from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine
from db.db_schema import stock_data
import pandas as pd
from dotenv import load_dotenv
import dotenv
import os

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DATABASE_USERNAME')}:{os.getenv('DATABASE_PASSWORD')}@{os.getenv('DATABASE_HOSTNAME')}:{os.getenv('DATABASE_PORT')}/{os.getenv('DATABASE_NAME')}"
).connect()


def insert_stock_data(df: pd.DataFrame):
    data = df.to_dict(orient="records")
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
    engine.execute(stmt, data)
    engine.commit()
