import time
from screener_parser import Excel_Helper
from db.db_ops import insert_stock_data
from db.db_schema import stock_data

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config.logger import logger
from dotenv import load_dotenv
import os
from config.utils import parse_section, calculate_trends

load_dotenv()


class Screener:
    def __init__(self):
        self.login_url = "https://www.screener.in/login/"
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,"
                      "*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "max-age=0",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.screener.in",
            "Referer": "https://www.screener.in/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/140.0.0.0 Safari/537.36",
        }
        self.symbol_url = "https://www.screener.in/api/company/search/"
        self.excel_helper = Excel_Helper()
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
            logger.info(f"Company URL : {url}")
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

    def read_excel(self, filepath, symbol):
        """
        Read file, parse sections (pnl, balance, quarters, cashflow),
        combine them (wide), then melt to final long timeseries.
        """
        try:
            dfs = pd.read_excel(filepath, sheet_name="Data Sheet", header=None)
            if dfs.empty:
                logger.error(f"Exel file is empty")
                return pd.DataFrame()
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            return pd.DataFrame()
        except Exception as e:
            logger.error("Error while reading the Excel file: %s", e)
            return pd.DataFrame()
        try:
            pnl_df = parse_section(dfs, "PROFIT & LOSS", "Quarters", "PNL")
            bal_sh_df = parse_section(dfs, "BALANCE SHEET", "CASH FLOW:", "Balance Sheet")
            q_df = parse_section(dfs, "Quarters", "BALANCE SHEET", "Quarters")
            cash_df = parse_section(dfs, "CASH FLOW:", " Adjusted Equity Shares in Cr", "Cash Flow")
        except Exception as e:
            logger.error(f"Unable to parse section: {e}")
            return pd.DataFrame()
        try:
            annual_combined = self.excel_helper.combine({
                "pnl": pnl_df,
                "balance": bal_sh_df,
                "cashflow": cash_df
            }, period_code="A")

            if annual_combined.empty:
                logger.error("Annual Combined report is empty")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error while combining annual data: {e}")
            return pd.DataFrame()
        try:
            latest_date = annual_combined.index.max()
            trend_data = calculate_trends(annual_combined)
            trend_data['timestamp'] = latest_date
            trend_data['timestamp'] = pd.to_datetime(trend_data['timestamp'])
            if 'timestamp' in trend_data.columns:
                trend_data = trend_data.set_index('timestamp')
            else:
                raise KeyError("Timestamp not found")
            trend_data.index = pd.to_datetime(trend_data.index, errors="coerce")

            annual_combined = annual_combined[~annual_combined.index.isna()]
        except Exception as e:
            logger.error(f"Error while calculating trend data: {e}")

        quarterly_combined = self.excel_helper.combine({
            "quarters": q_df
        }, period_code="Q")
        quarterly_combined = quarterly_combined[~quarterly_combined.index.isna()]
        combined_wide = pd.concat([annual_combined, quarterly_combined, trend_data], axis=1)

        final_ts = self.excel_helper.melt_combined(combined_wide, symbol)
        insert_stock_data(table=stock_data, data_dict=final_ts.to_dict(orient='records'), retry=3, wait_period=30)

        return final_ts


if __name__ == "__main__":
    screen = Screener()
    # screen.login()
    # symbol_url = screen.fetch_symbol("ACC")
    company_name = ["ACC", "RELIANCE", "BANKINDIA", "VBL", "MAZDOCK", "JIOFIN"]
    for company in company_name:
        try:
            file = screen.fetch_data(company)
            if not file:
                logger.warning(f"Skipping {company}- File not found")
                continue
            dfs = screen.read_excel(file, company)
            if dfs.empty:
                logger.warning(f"Unable to process data- {company}")
                continue
            time.sleep(30)
        except Exception as e:
            logger.error(f"Unable to download files: {e}")
    # file = screen.fetch_data("GLOTTIS")
    # dfs = screen.read_excel(file, "GLOTTIS")
    # print(dfs.to_csv("Glottis2.csv", index=False))
    # print(screen.combine(dfs))
    # screen.timesseries_data(dfs)
