import pandas as pd
from config.logger import logger
import numpy as np

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
        df = make_unique_columns(df)
        df = clean_df(df)
        df.index = pd.to_datetime(df.index, format="%Y-%m-%d", errors="coerce")
        df = df.apply(pd.to_numeric)

        return df

    except Exception as e:
        logger.error(f"Unable to parse section: {e}")


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    pd.set_option("future.no_silent_downcasting", True)
    # df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna(0.0)
    return df


def calculate_trends(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Sales growth (10/7/5/3-year + recent) and basic OPM / P/E stats.
    Assumes df is indexed by date (or at least sorted chronologically) and
    contains columns: 'sales_pnl', 'yearly OPM', 'price to earning' (or adjust names).
    """

    # Ensure chronological order
    df = df.sort_index()

    # Ensure the key columns are numeric
    if 'sales_pnl' not in df.columns:
        raise KeyError("sales_pnl column missing from df")

    df['sales_pnl'] = pd.to_numeric(df['sales_pnl'], errors='coerce')

    trends = {}
    years_list = [10, 7, 5, 3]

    if len(df) == 0:
        return pd.DataFrame([trends])

    last_pos = len(df) - 1
    latest_sales = df['sales_pnl'].iloc[last_pos]

    # SALES GROWTH using the start index logic you specified
    for n in years_list:
        # pick start position relative to last_pos
        start_pos = max(0, last_pos - n)  # maps -> 10y: 0 (if len==10), 7y: 2, 5y:4, 3y:6 in your example
        intervals = last_pos - start_pos  # number of intervals between start and last
        key = f"Sales Growth_{n}Y"

        # can't compute if no interval or missing/invalid numbers
        if intervals <= 0:
            trends[key] = np.nan
            continue

        start_sales = df['sales_pnl'].iloc[start_pos]

        if pd.isna(start_sales) or pd.isna(latest_sales) or start_sales <= 0:
            trends[key] = np.nan
            continue

        growth = (latest_sales / start_sales) ** (1.0 / intervals) - 1.0
        trends[key] = round(growth * 100, 2)  # percent

    # RECENT: use last 3 points (intervals = 2)
    if len(df) >= 3:
        start_pos_recent = last_pos - 2
        start_sales_recent = df['sales_pnl'].iloc[start_pos_recent]
        if pd.notna(start_sales_recent) and start_sales_recent > 0 and pd.notna(latest_sales):
            recent_growth = (latest_sales / start_sales_recent) ** (1.0 / 2) - 1.0
            trends['Sales Growth_RECENT'] = round(recent_growth * 100, 2)
        else:
            trends['Sales Growth_RECENT'] = np.nan
    else:
        trends['Sales Growth_RECENT'] = np.nan

    return pd.DataFrame([trends])
