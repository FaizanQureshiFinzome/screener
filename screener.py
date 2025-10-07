import re

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

from config.logger import logger
from dotenv import load_dotenv
import os
from config.utils import parse_section, calculate_trends, detect_year_end

load_dotenv()


class Screener:
    def __init__(self):
        self.login_url = "https://www.screener.in/login/"
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "max-age=0",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.screener.in",
            "Referer": "https://www.screener.in/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        }
        self.symbol_url = "https://www.screener.in/api/company/search/"

        self.email = os.getenv("SCREENER_EMAIL")
        self.password = os.getenv("SCREENER_PASSWORD")
        self.csrfmiddlewaretoken = ""
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def is_logged_in(self):
        return "sessionid" in self.session.cookies

    def login(self):
        if self.is_logged_in():
            logger.info("Already logged in.")
            return

        try:
            r = self.session.get(self.login_url, headers=self.headers)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            input_token = soup.find("input", {"name": "csrfmiddlewaretoken"})
            if input_token:
                self.csrfmiddlewaretoken = input_token.get("value")
                logger.info(f"middleware token: {self.csrfmiddlewaretoken}")
            else:
                logger.error("Login failed, csrfmiddlewaretoken not found.")
                return

            payload = {"username": self.email, "password": self.password,
                       "csrfmiddlewaretoken": self.csrfmiddlewaretoken}

            login_resp = self.session.post(self.login_url, data=payload,
                                           headers={**self.headers, "Referer": self.login_url})
            login_resp.raise_for_status()
            logger.info(login_resp.status_code)
            if "sessionid" in self.session.cookies:
                logger.info(f"Session ID: {self.session.cookies['sessionid']}")

            else:
                logger.error("Login failed, sessionid not found.")

        except requests.exceptions.RequestException as e:
            logger.error(f"Login failed: {e}")

        except Exception as e:
            logger.error(f"Unable to login: {e}")

    def fetch_symbol(self, symbol):
        try:
            param = {
                "q": symbol,
                "v": 3,
                "fts": 1
            }
            data = self.session.get(self.symbol_url, params=param)
            data.raise_for_status()
            company_url = (data.json()[0]['url'])
            logger.info(f"Fetched company url: {company_url}")
            return company_url

        except requests.exceptions.RequestException as e:
            logger.error(f"Something went wrong while fetching symbol URL: {e}")
        except (ValueError, IndexError, KeyError) as e:
            logger.error(f"Something went wrong while fetching symbol URL: {e}")
        except Exception as e:
            logger.error(f"Something went wrong while fetching symbol URL: {e}")
        return None

    def fetch_data(self, symbol):
        try:
            self.login()

            company_url = self.fetch_symbol(symbol=symbol)
            url = f"https://www.screener.in{company_url}"
            logger.info(url)
            res = self.session.get(url)
            res.raise_for_status()

            soup = BeautifulSoup(res.text, "html.parser")
            btn = soup.find("button", attrs={"aria-label": "Export to Excel"})
            if not btn or "formaction" not in btn.attrs:
                raise Exception("❌ Could not find export button on page")

            export_url = f"https://www.screener.in{btn['formaction']}"
            logger.info(f"Downloading from {export_url}")

            csrftoken = self.session.cookies.get("csrftoken")
            if not csrftoken:
                raise Exception("❌ csrftoken not found in cookies")

            headers = {
                **self.headers,
                "Referer": url,  # must match company page
                "X-CSRFToken": csrftoken,  # Django requires this
            }

            resp = self.session.post(
                export_url,
                headers=headers,
                cookies=self.session.cookies,
                stream=True
            )
            logger.info(resp.status_code)

            if resp.status_code != 200:
                raise Exception(f"❌ Failed to download file. Status {resp.status_code}: {resp.text[:200]}")

            filepath = f"reports/export_{company_url.split('/')[2]}.xlsx"
            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            return filepath
        except Exception as e:
            logger.error(f"Something went wrong while fetching data: {e}")
        return None

    def melt_combined(self, combined_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Take the wide combined DataFrame (index = dates, columns like
        'adjusted equity shares in cr_cashflow', 'borrowings_balance', 'sales_quarters', ...)
        and melt it into the final long timeseries format:
        timestamp, period_start, period_end, period_code, metric_name, metric_value, symbol
        """
        if combined_df is None or combined_df.empty:
            return pd.DataFrame(columns=[
                "timestamp", "period_start", "period_end", "period_code", "metric_name", "metric_value", "symbol"
            ])

        df = combined_df.copy()

        # Ensure index is datetime; if not, try to coerce a timestamp column or the index
        if not isinstance(df.index, pd.DatetimeIndex):
            # prefer an explicit timestamp-like column if present
            date_cols = [c for c in df.columns if "date" in c or "timestamp" in c]
            if date_cols:
                df[date_cols[0]] = pd.to_datetime(df[date_cols[0]], errors="coerce")
                df = df.set_index(date_cols[0])
            else:
                # try converting the current index
                try:
                    df.index = pd.to_datetime(df.index, errors="coerce")
                except Exception:
                    pass

        # drop rows where index is NaT
        df = df[~df.index.isna()].copy()

        # Reset index to get timestamp column
        df_reset = df.reset_index()
        df_reset = df_reset.rename(columns={df_reset.columns[0]: "timestamp"})

        # Melt into long form
        df_long = df_reset.melt(id_vars=["timestamp"], var_name="metric", value_name="metric_value")

        # Convert numeric where possible and drop NaNs
        df_long["metric_value"] = pd.to_numeric(df_long["metric_value"], errors="coerce")
        df_long = df_long.dropna(subset=["metric_value", "timestamp"]).copy()

        # Split metric into base name + suffix (pnl | quarters | balance | cashflow)
        def split_metric(col: str):
            m = re.match(r"(?P<base>.+)_(?P<suffix>pnl|quarters|balance|cashflow)$", col)
            if m:
                return m.group("base").strip(), m.group("suffix")
            # no suffix -> assume annual (A)
            return col.strip(), None

        parts = df_long["metric"].apply(split_metric)
        df_long[["metric_name", "suffix"]] = pd.DataFrame(parts.tolist(), index=df_long.index)

        # Determine period code
        df_long["period_code"] = df_long["suffix"].map({
            "quarters": "Q",
            "pnl": "A",
            "balance": "A",
            "cashflow": "A"
        }).fillna("A")

        # Ensure timestamp is datetime
        df_long["timestamp"] = pd.to_datetime(df_long["timestamp"], errors="coerce")

        # Compute period_start and period_end
        mask_q = df_long["period_code"] == "Q"
        mask_a = df_long["period_code"] == "A"

        # Quarter start (vectorized)
        if mask_q.any():
            df_long.loc[mask_q, "period_start"] = df_long['timestamp'].apply(
                lambda x: x - relativedelta(months=2, day=1))
        # Annual start = Jan 1 of that year (vectorized)
        if mask_a.any():
            df_long.loc[mask_a, "period_start"] = df_long['timestamp'].apply(
                lambda x: x - relativedelta(months=11, day=1))
            # period_end is the timestamp itself
        df_long["period_end"] = df_long["timestamp"]
        df_long.loc[mask_q, 'period_code'] = np.where(
            df_long.loc[mask_q, 'period_end'].dt.month == 6, "Q1",
            np.where(df_long.loc[mask_q, 'period_end'].dt.month == 9, "Q2",
                     np.where(df_long.loc[mask_q, 'period_end'].dt.month == 12, "Q3",
                              np.where(df_long.loc[mask_q, 'period_end'].dt.month == 3, "Q4", "Q")))
        )
        # Final columns & cleanup
        df_long = df_long[[
            "timestamp", "period_start", "period_end", "period_code", "metric_name", "metric_value"
        ]].copy()
        df_long["symbol"] = symbol

        # sort for nicer output
        df_long = df_long.sort_values(["timestamp", "metric_name"]).reset_index(drop=True)
        return df_long

    def read_excel(self, filepath, symbol):
        """
        Read file, parse sections (pnl, balance, quarters, cashflow),
        combine them (wide), then melt to final long timeseries.
        """
        try:
            dfs = pd.read_excel(filepath, sheet_name="Data Sheet", header=None)
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            return pd.DataFrame()
        except Exception as e:
            logger.error("Error while reading the Excel file: %s", e)
            return pd.DataFrame()

        pnl_df = parse_section(dfs, "PROFIT & LOSS", "Quarters", "PNL")
        bal_sh_df = parse_section(dfs, "BALANCE SHEET", "CASH FLOW:", "Balance Sheet")
        q_df = parse_section(dfs, "Quarters", "BALANCE SHEET", "Quarters")
        cash_df = parse_section(dfs, "CASH FLOW:", " Adjusted Equity Shares in Cr", "Cash Flow")

        annual_combined = self.combine({
            "pnl": pnl_df,
            "balance": bal_sh_df,
            "cashflow": cash_df
        }, period_code="A")

        quarterly_combined = self.combine({
            "quarters": q_df
        }, period_code="Q")

        combined_wide = pd.concat([annual_combined, quarterly_combined], axis=1)

        final_ts = self.melt_combined(combined_wide, symbol)

        return final_ts

    def combine(self, dfs, period_code="A"):
        frames = []
        for name, df in dfs.items():
            df = pd.DataFrame(df)
            df.columns = df.columns.str.lower()
            df = df.add_suffix(f"_{name}")
            frames.append(df)

        combined_df = pd.concat(frames, axis=1)

        combined_df = combined_df.rename(columns={'price:_cashflow': 'price'})
        if 'derived:_cashflow' in combined_df.columns:
            combined_df = combined_df.drop('derived:_cashflow', axis=1)

        for col in combined_df.columns:
            combined_df[col] = (
                combined_df[col]
                .astype(str)
                .str.replace(',', '', regex=False)
                .str.strip()
            )
            try:
                combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
            except Exception:
                pass

        for col in combined_df.columns:
            if 'report date' in col:
                combined_df['timestamp'] = pd.to_datetime(combined_df[col], errors='coerce')
                break

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

            combined_df['yearly OPM'] = np.where(
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
            combined_df['quarterly OPM_quarters'] = np.where(
                combined_df['sales_quarters'] > 0,
                np.round(
                    combined_df['operating profit_quarters'] / combined_df['sales_quarters'] * 100),
                0
            )

        combined_df['period_code'] = period_code
        return combined_df


if __name__ == "__main__":
    screen = Screener()
    # screen.login()
    # screen.fetch_symbol("maruti se")
    # file = screen.fetch_data("ACC")
    file = "reports/export_ACC.xlsx"
    dfs = screen.read_excel(file, "ACC")
    print(dfs.to_csv("acc4.csv", index=False))
    # print(screen.combine(dfs))
    # screen.timesseries_data(dfs)
