import pandas as pd
from config.logger import logger
import numpy as np
import re


# Split metric
def split_metric(col: str):
    m = re.match(r"(?P<base>.+)_(?P<suffix>pnl|quarters|balance|cashflow)$", col)
    if m:
        return m.group("base").strip(), m.group("suffix")
    return col.strip(), None


def detect_year_end(df: pd.DataFrame) -> str:
    annual_month = df.loc[df['period_code'] == 'A', 'timestamp'].dt.month.unique()
    if 12 in annual_month:
        return "FY-DEC"
    elif 3 in annual_month:
        return "FY-MAR"


def make_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = []
    seen = {}
    for i, col in enumerate(df.columns):
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}.{seen[col]}")
        else:
            seen[col] = 0
            new_cols.append(col)
    df.columns = new_cols
    return df


def parse_section(dfs, start_block, end_block=None, section_name=""):
    try:
        start = dfs.index[dfs[0] == start_block][0]
        header_row = start + 1
        if end_block:
            try:
                end = dfs.index[dfs[0] == end_block][0]
            except IndexError:
                end = len(dfs)
        else:
            end = len(dfs)

        block = dfs.iloc[header_row: end].reset_index(drop=True)
        block.columns = block.iloc[0]
        df = block.drop(0).reset_index(drop=True)
        df = df.dropna(how="all")
        df = make_unique_columns(df)

        if "Report Date" in df.columns:
            df = df.set_index("Report Date")
        else:
            raise KeyError(f"Report Date column not found in {section_name} section!")

        df = df.T
        if "Total" in df.columns:
            df = df.drop("Total", axis=1)
        df = df[~df.index.isna()]
        df = make_unique_columns(df)
        df = clean_df(df)
        df.index = pd.to_datetime(df.index, format="%Y-%m-%d", errors="coerce")
        df = df.apply(pd.to_numeric)
        df = clean_df(df)
        return df

    except Exception as e:
        logger.error(f"Unable to parse section: {e}")
        return pd.DataFrame()


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    pd.set_option("future.no_silent_downcasting", True)
    # df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna(0.0)
    return df


def calculate_trends(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Ensure chronological order
        df = df.sort_index()

        # Ensure the key columns are numeric
        if 'sales_pnl' and 'OPM' not in df.columns:
            raise KeyError("sales_pnl column missing from df")

        df['sales_pnl'] = pd.to_numeric(df['sales_pnl'], errors='coerce')

        trends = {}
        years_list = [10, 7, 5, 3]

        if len(df) == 0:
            return pd.DataFrame([trends])

        last_pos = len(df) - 1
        latest_sales = df['sales_pnl'].iloc[last_pos]

        for n in years_list:
            start_pos = max(0, last_pos - n)
            intervals = last_pos - start_pos
            sales_key = f"Sales Growth_{n}Y"
            opm_key = f"OPM{n}Y"

            if intervals <= 0:
                trends[sales_key] = np.nan
                continue

            start_sales = df['sales_pnl'].iloc[start_pos]

            # 🚨 Handle zero or invalid start
            if pd.isna(start_sales) or pd.isna(latest_sales):
                trends[sales_key] = np.nan
                continue

            if latest_sales == 0:
                # If the start value is 0, don't calculate growth — set to 0 directly
                trends[sales_key] = 0.0
                continue

            if latest_sales < 0:
                # Optional: handle negative start (unusual for sales)
                trends[sales_key] = np.nan
                continue

            growth = (latest_sales / start_sales) ** (1.0 / intervals) - 1.0
            trends[sales_key] = round(growth * 100, 2)

            # net_sales = df['sales_pnl'].sum()

        return pd.DataFrame([trends])

    except Exception as e:
        logger.error(f"Empty trend data {e}")
        return pd.DataFrame()
