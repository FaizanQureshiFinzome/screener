from sqlalchemy import Column, DateTime, String, Integer, VARCHAR, MetaData, Table, Float, func, UniqueConstraint

metadata_obj = MetaData()

stock_data = Table(
    "stock_data",
    metadata_obj,
    Column('timestamp', DateTime),
    Column('period_start', DateTime, nullable=False),
    Column('period_end', DateTime, nullable=False),
    Column('period_code', String, nullable=False),
    Column('fiscal_type', String, nullable=False),
    Column('metric_name', String, nullable=False),
    Column('metric_value', Float, nullable=False),
    Column('symbol', String, nullable=False),
    Column('created_at', DateTime, server_default=func.now()),
    Column('updated_at', DateTime, server_default=func.now()),
    UniqueConstraint('symbol', 'timestamp', 'metric_name', 'period_code', name='uq_symbol_timeseries_metric')

)