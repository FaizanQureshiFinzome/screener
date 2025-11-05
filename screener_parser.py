import numpy as np
import pandas as pd
from config.logger import logger
from dateutil.relativedelta import relativedelta
from config.utils import split_metric


class Excel_Helper:
    @staticmethod
    def melt_combined(combined_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        if combined_df is None or combined_df.empty:
            return pd.DataFrame(columns=[
                "timestamp", "period_start", "period_end", "period_code", "fiscal_type", "metric_name", "metric_value",
                "symbol"
            ])

        df = combined_df.copy()

        # Ensure index is datetime
        if not isinstance(df.index, pd.DatetimeIndex):
            date_cols = [c for c in df.columns if "date" in c or "timestamp" in c]
            if date_cols:
                df[date_cols[0]] = pd.to_datetime(df[date_cols[0]], errors="coerce")
                df = df.set_index(date_cols[0])
            else:
                try:
                    df.index = pd.to_datetime(df.index, errors="coerce")
                except Exception as e:
                    logger.error("Error while converting index to datetime: %s", e)

        df = df[~df.index.isna()].copy()

        # Reset index
        df_reset = df.reset_index().rename(columns={df.reset_index().columns[0]: "timestamp"})

        df_long = df_reset.melt(id_vars=["timestamp"], var_name="metric", value_name="metric_value")
        df_long["metric_value"] = pd.to_numeric(df_long["metric_value"], errors="coerce")
        df_long = df_long.dropna(subset=["metric_value", "timestamp"]).copy()

        parts = df_long["metric"].apply(split_metric)
        df_long[["metric_name", "suffix"]] = pd.DataFrame(parts.tolist(), index=df_long.index)

        # Determine period_code
        df_long["period_code"] = df_long["suffix"].map({
            "quarters": "Q",
            "pnl": "A",
            "balance": "A",
            "cashflow": "A"
        }).fillna("A")

        df_long["timestamp"] = pd.to_datetime(df_long["timestamp"], errors="coerce")

        mask_q = df_long["period_code"] == "Q"
        mask_a = df_long["period_code"] == "A"

        df_long["period_end"] = df_long["timestamp"]

        # Period start & end
        if mask_q.any():
            df_long.loc[mask_q, "period_start"] = df_long['timestamp'].apply(
                lambda x: x - relativedelta(months=2, day=1))

        if mask_a.any():
            df_long.loc[mask_a, "period_start"] = df_long['timestamp'].apply(
                lambda x: x - relativedelta(months=11, day=1))

        # --- Fiscal year mapping from annual data ---
        fiscal_year_map_raw = (
            df_long[mask_a]
            .groupby(df_long.loc[mask_a, 'timestamp'].dt.year)['period_end']
            .max()
            .dt.month
            .to_dict()
        )

        # --- Forward fill the fiscal year map dynamically ---
        all_years = sorted(df_long['timestamp'].dt.year.unique())
        fiscal_year_map = {}
        last_known = None
        for y in all_years:
            if y in fiscal_year_map_raw:
                last_known = fiscal_year_map_raw[y]
            if last_known is not None:
                fiscal_year_map[y] = last_known
            else:
                fiscal_year_map[y] = 3  # default to March if nothing known

        df_long['fiscal_year_end'] = df_long['timestamp'].dt.year.map(fiscal_year_map)
        df_long.loc[df_long['period_code'] != 'Q', 'period_code'] = 'A'

        # --- Quarter logic for FY-MAR ---
        march_end = (df_long['fiscal_year_end'] == 3) & (df_long['period_code'] != 'A')
        df_long.loc[march_end & (df_long['period_end'].dt.month == 6), 'period_code'] = 'Q1'
        df_long.loc[march_end & (df_long['period_end'].dt.month == 9), 'period_code'] = 'Q2'
        df_long.loc[march_end & (df_long['period_end'].dt.month == 12), 'period_code'] = 'Q3'
        df_long.loc[march_end & (df_long['period_end'].dt.month == 3), 'period_code'] = 'Q4'

        # --- Quarter logic for FY-DEC ---
        dec_end = (df_long['fiscal_year_end'] == 12) & (df_long['period_code'] != 'A')
        df_long.loc[dec_end & (df_long['period_end'].dt.month == 3), 'period_code'] = 'Q1'
        df_long.loc[dec_end & (df_long['period_end'].dt.month == 6), 'period_code'] = 'Q2'
        df_long.loc[dec_end & (df_long['period_end'].dt.month == 9), 'period_code'] = 'Q3'
        df_long.loc[dec_end & (df_long['period_end'].dt.month == 12), 'period_code'] = 'Q4'

        # Fiscal type
        df_long['fiscal_type'] = np.where(df_long['fiscal_year_end'] == 3, 'FY-MAR', 'FY-DEC')

        # Final columns
        df_long = df_long[[
            "timestamp", "period_start", "period_end", "period_code", "fiscal_type", "metric_name", "metric_value"
        ]].copy()
        df_long["symbol"] = symbol

        df_long = df_long.sort_values(["timestamp", "metric_name"]).reset_index(drop=True)

        return df_long

    @staticmethod
    def combine(dfs, period_code="A"):
        frames = []
        for name, df in dfs.items():
            df = pd.DataFrame(df)
            df.columns = df.columns.str.lower()
            df = df.add_suffix(f"_{name}")
            frames.append(df)

        combined_df = pd.concat(frames, axis=1)

        if 'price:_cashflow' in combined_df.columns:
            combined_df = combined_df.rename(columns={'price:_cashflow': 'price'})
        if 'derived:_cashflow' in combined_df.columns:
            combined_df = combined_df.drop('derived:_cashflow', axis=1)

        if period_code == 'A':
            combined_df['expenses_pnl'] = (
                    combined_df['raw material cost_pnl'] +
                    combined_df['power and fuel_pnl'] +
                    combined_df['other mfr. exp_pnl'] +
                    combined_df['employee cost_pnl'] +
                    combined_df['selling and admin_pnl'] +
                    combined_df['other expenses_pnl'] +
                    -1 * combined_df['change in inventory_pnl']
            )

            combined_df['operating_profit_pnl'] = combined_df['sales_pnl'] - combined_df['expenses_pnl']

            combined_df['dividend_payout_pnl'] = np.where(
                combined_df['net profit_pnl'] > 0,
                round((combined_df['dividend amount_pnl'] / combined_df['net profit_pnl']) * 100, 2),
                0
            )

            combined_df['EPS'] = np.where(
                combined_df['adjusted equity shares in cr_cashflow'] > 0,
                round(combined_df['net profit_pnl'] / combined_df['adjusted equity shares in cr_cashflow'], 2),
                0
            )

            combined_df['OPM'] = np.where(
                combined_df['operating_profit_pnl'] > 0,
                np.round(
                    round((combined_df['operating_profit_pnl'] / combined_df['sales_pnl']) * 100, 2)),
                0
            )

            combined_df['ROE'] = np.where(
                (combined_df['equity share capital_balance'] + combined_df['reserves_balance']) > 0,
                np.round(
                    round((combined_df['net profit_pnl'] / (
                            combined_df['equity share capital_balance'] + combined_df['reserves_balance'])) * 100, 2)),
                0
            )

            combined_df['price_to_earning'] = np.where(
                combined_df['EPS'] > 0,
                round(combined_df['price'] / combined_df['EPS'], 2),
                0
            )

            combined_df['working_capital'] = (
                    combined_df['other assets_balance'] - combined_df['other liabilities_balance']
            )

            combined_df['debtor_days'] = np.where(
                combined_df['sales_pnl'] > 0,
                round(combined_df['receivables_balance'] / (combined_df['sales_pnl'] / 365), 2),
                0
            )

            combined_df['inventory_turnover'] = np.where(
                combined_df['inventory_balance'] > 0,
                round(combined_df['sales_pnl'] / combined_df['inventory_balance'], 2),
                0
            )

        elif period_code == 'Q':
            combined_df['OPM_quarters'] = np.where(
                combined_df['sales_quarters'] > 0,
                np.round(
                    combined_df['operating profit_quarters'] / combined_df['sales_quarters'] * 100),
                0
            )

        combined_df['period_code'] = period_code
        return combined_df
