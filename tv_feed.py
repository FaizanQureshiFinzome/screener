# Copyright (c) 2025 Finzome Technologies Private Limited - All Rights Reserved

# Unauthorized copying of this file, via any medium is strictly prohibited

# Proprietary and confidential

# Written by Finzome Technologies <admin@finzome.com>, Last Modified: 13-11-2025

import logging

import random



import string

import pandas as pd
import requests

from websocket import create_connection

import json
import datetime

logger = logging.getLogger(__name__)


class TvDatafeed:
    __ws_headers = json.dumps({"Origin": "https://data.tradingview.com"})

    __ws_timeout = 5

    def __init__(self) -> None:

        """Create TvDatafeed object"""

        self.ws_debug = False

        self.token = "unauthorized_user_token"

        self.ws = None

        self.session = self.__generate_session()

        self.news_url = 'https://news-mediator.tradingview.com/public/view/v1/symbol'

        self.technicals_url = 'https://scanner.tradingview.com/symbol'

        self.docs_url = 'https://news-mediator.tradingview.com/public/doc-screener/v1/documents'

        self.headers = {
            'sec-ch-ua-platform': '"Windows"',
            'Referer': 'https://in.tradingview.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
        }

    def __create_connection(self):

        logging.debug("creating websocket connection")

        self.ws = create_connection(

            "wss://data.tradingview.com/socket.io/websocket", headers=self.__ws_headers)

    @staticmethod
    def __generate_session():

        stringLength = 12

        letters = string.ascii_lowercase

        random_string = "".join(random.choice(letters)

                                for i in range(stringLength))

        return "qs_" + random_string

    @staticmethod
    def __prepend_header(st):

        return "~m~" + str(len(st)) + "~m~" + st

    @staticmethod
    def __construct_message(func, param_list):

        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    def __create_message(self, func, paramList):

        return self.__prepend_header(self.__construct_message(func, paramList))

    def __send_message(self, func, args):

        m = self.__create_message(func, args)

        if self.ws_debug:
            logger.info(m)

        self.ws.send(m)

    # @staticmethod
    # def __decode_quote(raw_data):
    #
    #     """
    #
    #     The __decode_quote function takes in a string of raw data and returns a dictionary with the following keys:
    #
    #         - price_target_average
    #
    #         - price_target_high
    #
    #         - price_target_low
    #
    #     :param raw_data: Pass in the raw data from the api call
    #
    #     :return: A dictionary of the average, high and low price targets for a given stock
    #
    #     """
    #
    #     _patterns = {
    #
    #         "_high_low_avg": r'"price_target_(?:average|(?:high|low))":(?:[0-9]*\.[0-9]+|(?:[0-9]+))',
    #
    #         "close_": r'"daily-bar":\{"close":(?:"[0-9]*\.[0-9]+"|(?:"[0-9]+"))',
    #
    #         "prev": r'"prev_close_price":[0-9]*\.[0-9]+',
    #
    #         "lp": r'"lp":(?:[0-9]*\.[0-9]+|(?:[0-9]+))',
    #
    #         "price_to_sales": r'"price_sales_current":(?:[0-9]*\.[0-9]+|(?:[0-9]+))',
    #
    #         "lp_time": r'"lp_time":[0-9]+',
    #
    #         "rtc": r'"rtc":[0-9]*\.[0-9]+',
    #
    #         "rtc_time": r'"rtc_time":[0-9]+',
    #
    #         "industry": r'"industry"\s*:\s*"([^"]*)"',
    #
    #         "name": r'"local_description"\s*:\s*"([^"]*)"',
    #
    #         "revenue": r'"revenues_fy_h":\[[^\]]*\]',
    #
    #         "dividend": r'"dividend_yield_recent":[0-9]*\.[0-9]+',
    #
    #         "quarterly_revenue": r'"revenues_fq_h":\[[^\]]*\]',
    #
    #         "earnings": r'"earnings_fy_h":\[[^\]]*\]',
    #
    #         "quarterly_earnings": r'"earnings_fq_h":\[[^\]]*\]',
    #
    #         "return_on_equity": r'"return_on_equity_fy_h":\[[^\]]*\]',
    #         "quarterly_return_on_equity": r'"return_on_equity_fq_h":\[[^\]]*\]',
    #
    #         "return_on_assets": r'"return_on_assets_fy_h":\[[^\]]*\]',
    #         "quarterly_return_on_assets": r'"return_on_assets_fq_h":\[[^\]]*\]',
    #
    #         "return_on_invested_capital": r'"return_on_invested_capital_fy_h":\[[^\]]*\]',
    #         "quarterly_return_on_invested_capital": r'"return_on_invested_capital_fq_h":\[[^\]]*\]',
    #
    #         "gross_margin": r'"gross_margin_fy_h":\[[^\]]*\]',
    #         "quarterly_gross_margin": r'"gross_margin_fq_h":\[[^\]]*\]',
    #
    #         "operating_margin": r'"operating_margin_fy_h":\[[^\]]*\]',
    #         "quarterly_operating_margin": r'"operating_margin_fq_h":\[[^\]]*\]',
    #
    #
    #
    #
    #     }
    #
    #     # pattern = r'"price_target_[A-Za-z]+":[0-9]+'
    #
    #     # pattern = r'"price_target_(?:average|(?:high|low))":[0-9]+'
    #
    #     # pattern = r'"rtc_time":[0-9]*\.[0-9]+'
    #
    #     data = {}
    #
    #     for pattern in _patterns.values():
    #
    #         try:
    #
    #             matches = re.findall(pattern, raw_data)
    #
    #             for _sp in matches:
    #
    #                 _sp = _sp.split(":")
    #
    #                 if pattern == _patterns['industry']:
    #
    #                     _tmp = ''
    #
    #                     for i in range(0, len(_sp)):
    #                         _tmp += _sp[i].replace('"', '')
    #
    #                     data['industry'] = _tmp
    #
    #                 elif pattern == _patterns['name']:
    #
    #                     _tmp = ''
    #
    #                     for i in range(0, len(_sp)):
    #                         _tmp += _sp[i].replace('"', '')
    #
    #                     data['name'] = _tmp
    #
    #                 elif pattern == _patterns['revenue']:
    #
    #                     revenue = json.loads(matches[0].split('"revenues_fy_h":')[1])
    #
    #                     data['revenue'] = revenue
    #
    #                 elif pattern == _patterns['quarterly_revenue']:
    #
    #                     revenue = json.loads(matches[0].split('"revenues_fq_h":')[1])
    #
    #                     data['quarterly_revenue'] = revenue
    #
    #                 elif pattern == _patterns['earnings']:
    #
    #                     revenue = json.loads(matches[0].split('"earnings_fy_h":')[1])
    #
    #                     data['earnings'] = revenue
    #
    #                 elif pattern == _patterns['quarterly_earnings']:
    #
    #                     revenue = json.loads(matches[0].split('"earnings_fq_h":')[1])
    #
    #                     data['quarterly_earnings'] = revenue
    #                 elif pattern == _patterns['return_on_equity']:
    #
    #                     revenue = json.loads(matches[0].split('"return_on_equity_fy_h":')[1])
    #
    #                     data['return_on_equity'] = revenue
    #
    #                 else:
    #
    #                     data[_sp[-2].replace('{', '').replace('"', '')] = float(_sp[-1].replace('"', ''))  # close,prev
    #
    #         except Exception as exc:
    #
    #             logger.error(exc)
    #
    #     return data

    @staticmethod
    def __decode_quote(raw_data):
        import re, json

        data = {}

        # ----------------------------
        # 1) Extract simple numeric/str fields
        # ----------------------------
        simple_fields = re.findall(r'"([a-zA-Z0-9_]+)":("?[^",{}\[\]]+"?)', raw_data)

        for key, val in simple_fields:
            # skip fundamentals arrays (handled below)
            if key.endswith("_fy_h") or key.endswith("_fq_h"):
                continue
            if key in ["industry", "local_description"]:
                data[key] = val.replace('"', '')
                continue
            try:
                data[key] = float(val)
            except:
                data[key] = val.replace('"', '')

        # ----------------------------
        # 2) Extract ALL fundamental arrays (_fy_h and _fq_h)
        # ----------------------------
        array_pattern = r'"([a-zA-Z0-9_]+_(?:fy|fq)_h)":(\[[^\]]*\])'

        for key, arr in re.findall(array_pattern, raw_data):
            try:
                data[key] = json.loads(arr)
            except:
                pass

        return data

    @classmethod
    def ts_to_date(cls, ts):
        return datetime.datetime.utcfromtimestamp(ts).date()

    @classmethod
    def ts_to_year(cls, ts):
        return cls.ts_to_date(ts).year

    # @classmethod
    # def ts_to_quarter(cls, ts):
    #     d = cls.ts_to_date(ts)
    #     q = (d.month - 1) // 3 + 1
    #     return f"{d.year}-Q{q}"

    def build_forcast_df(self, tv_dict: dict):

        records = []

        def extract_block(block, metric_name):
            if block is None:
                return

            for row in block:
                records.append({
                    "period": row['FiscalPeriod'],
                    "period_type": "Q" if "Q" in row['FiscalPeriod'] else "A",
                    "actual": row['Actual'],
                    "estimate": row['Estimate'],
                    "is_reported": row['IsReported'],
                    "metric": metric_name
                })

        extract_block(tv_dict.get("revenues_fy_h"), "Revenue")
        extract_block(tv_dict.get("earnings_fy_h"), "Earnings")

        extract_block(tv_dict.get("revenues_fq_h"), "Revenue")
        extract_block(tv_dict.get("earnings_fq_h"), "Earnings")

        df = pd.DataFrame(records)
        df['surprise'] = ((df['actual'] - df['estimate']) / df['estimate']) * 100
        return df

    @staticmethod
    def __format_symbol(symbol: str, exchange):

        """

        The __format_symbol function takes a symbol and an exchange, and returns the formatted symbol.

        :param symbol: str: Pass the symbol of the stock

        :param exchange: Specify the exchange that the symbol is traded on

        :return: A string of the form &quot;exchange:symbol&quot;

        """

        symbol = f"{exchange}:{symbol}"

        return symbol

    def get_quote(self, symbol: str, exchange: str, create_file: bool = False):

        """

        The get_quote function takes in a symbol and an exchange, formats the symbol to be compatible with the API,

        creates a connection to the websocket server, sends messages to set data quality (low), authentication token (self.token),

        locale (en_IN) and creates a session for that particular quote request. It then adds symbols for which quotes are requested

        and receives raw data from the websocket server until it encounters either &quot;quote_completed&quot; or
&quot;no_such_symbol&quot;. The function then returns decoded quote data.

        :param symbol:str: Specify the symbol of the stock you want to get data for

        :param exchange:str: Specify the exchange on which the symbol is traded

        :return: The data in the form of a dictionary

        """

        symbol = self.__format_symbol(

            symbol=symbol, exchange=exchange

        )

        print(symbol)

        self.__create_connection()

        self.__send_message("set_auth_token", [self.token])

        self.__send_message("set_locale", ["en", "IN"])

        self.__send_message('set_data_quality', ["low"])

        self.__send_message("quote_create_session", [self.session])

        self.__send_message("quote_add_symbols", [self.session, symbol])

        raw_data = ""

        logger.debug(f"getting data for {symbol}...")

        while True:

            try:

                result = self.ws.recv()

                raw_data = raw_data + result + "\n"

            except Exception as e:

                logger.error(e)

                break

            if "quote_completed" in result:
                break

            if "no_such_symbol" in result:
                logger.error("Invalid symbol or the exchange")

                break

        if create_file:
            with open(f"{symbol.split(':')[-1]}.json", "w") as text_file:
                text_file.write(raw_data)

        return self.__decode_quote(raw_data)

    def parse_all_metrics_single_df(self, tv_dict: dict, include_metrics: list = None):

        records = []

        fy_ts = tv_dict.get("fiscal_period_end_fy_h", [])
        fq_ts = tv_dict.get("fiscal_period_fq_h", [])

        fy_periods = [TvDatafeed.ts_to_year(ts) for ts in fy_ts]
        fq_periods = fq_ts

        for key, val in tv_dict.items():

            # ANNUAL FY
            if key.endswith("_fy_h") and isinstance(val, list):
                metric = key.replace("_fy_h", "")
                if include_metrics and metric not in include_metrics:
                    continue

                N = min(len(val), len(fy_periods))
                for i in range(N):
                    records.append({
                        "period": fy_periods[i],
                        "period_type": "A",
                        "metric": metric,
                        "value": val[i]
                    })
                continue

            # QUARTERLY FQ
            if key.endswith("_fq_h") and isinstance(val, list):
                metric = key.replace("_fq_h", "")
                if include_metrics and metric not in include_metrics:
                    continue

                N = min(len(val), len(fq_periods))
                for i in range(N):
                    records.append({
                        "period": fq_periods[i],
                        "period_type": "Q",
                        "metric": metric,
                        "value": val[i]
                    })
                continue

            # TTM:
            if (key.endswith("_ttm") or key == "ttm") and isinstance(val, (int, float, list)):
                metric = key.replace("_ttm", "").replace("ttm", "ttm")

                if include_metrics and metric not in include_metrics:
                    continue

                # TTM values → usually single value or array of 1
                ttm_value = val[0] if isinstance(val, list) else val

                records.append({
                    "period": "TTM",
                    "period_type": "TTM",
                    "metric": metric,
                    "value": ttm_value
                })
                continue

            # Current:
            if (key.endswith("_current") or key == "current") and isinstance(val, (int, float, list)):
                metric = key.replace("_current", "").replace("current", "current")

                if include_metrics and metric not in include_metrics:
                    continue

                # current values → usually single value or array of 1
                current_value = val[0] if isinstance(val, list) else val

                records.append({
                    "period": "current",
                    "period_type": "current",
                    "metric": metric,
                    "value": current_value
                })
                continue

        return pd.DataFrame(records)

    def get_news(self, exchange, symbol):
        try:
            symbol = self.__format_symbol(symbol.upper(), exchange)
            params = {
                'filter': [
                    'lang:en',
                    f'symbol:{symbol}',
                ],
                'client': 'landing',
                'streaming': 'true'
            }
            response = requests.get(url=self.news_url, params=params, headers=self.headers)

            data = response.json().get('items', [])
            if not data:
                return pd.DataFrame()
            rows = []
            pre_fix = "https://www.tradingview.com"
            for item in data:
                row = {
                    "id": item.get('id'),
                    "title": item.get('title'),
                    "published": item.get('published'),
                    "link": pre_fix + item.get('storyPath')
                }

                rows.append(row)
            df = pd.DataFrame(rows)
            return df.to_csv('news.csv', index=False)

        except Exception as e:
            return f"{e}"

    def build_fields_param(self, timeframe: str):
        base_fields = {
            'Recommend.Other', 'Recommend.All', 'Recommend.MA', 'RSI', 'Stoch.K', 'Stoch.D', 'CCI20', 'ADX', 'ADX+DI',
            'ADX-DI', 'AO', 'Mom', 'MACD.macd', 'MACD.signal', 'Rec.Stoch.RSI', 'Stoch.RSI.K', 'Rec.WR', 'W.R',
            'Rec.BBPower', 'BBPower', 'Rec.UO', 'UO', 'EMA10', 'close', 'SMA10', 'EMA20', 'SMA20', 'EMA30', 'SMA30',
            'EMA50', 'SMA50', 'EMA100', 'SMA100', 'EMA200', 'SMA200', 'Rec.Ichimoku', 'Ichimoku.BLine', 'Rec.VWMA',
            'VWMA', 'Rec.HullMA9', 'HullMA9', 'Pivot.M.Classic.R3', 'Pivot.M.Classic.R2', 'Pivot.M.Classic.R1',
            'Pivot.M.Classic.Middle', 'Pivot.M.Classic.S1', 'Pivot.M.Classic.S2', 'Pivot.M.Classic.S3',
            'Pivot.M.Fibonacci.R3', 'Pivot.M.Fibonacci.R2', 'Pivot.M.Fibonacci.R1', 'Pivot.M.Fibonacci.Middle',
            'Pivot.M.Fibonacci.S1', 'Pivot.M.Fibonacci.S2', 'Pivot.M.Fibonacci.S3', 'Pivot.M.Camarilla.R3',
            'Pivot.M.Camarilla.R2', 'Pivot.M.Camarilla.R1', 'Pivot.M.Camarilla.Middle', 'Pivot.M.Camarilla.S1',
            'Pivot.M.Camarilla.S2', 'Pivot.M.Camarilla.S3', 'Pivot.M.Woodie.R3', 'Pivot.M.Woodie.R2',
            'Pivot.M.Woodie.R1', 'Pivot.M.Woodie.Middle', 'Pivot.M.Woodie.S1', 'Pivot.M.Woodie.S2', 'Pivot.M.Woodie.S3',
            'Pivot.M.Demark.R1', 'Pivot.M.Demark.Middle', 'Pivot.M.Demark.S1',
        }
        if timeframe == "":
            return ",".join(base_fields)

        joined = ",".join([f"{f}|{timeframe}" for f in base_fields])
        return joined

    def technicals(self, exchange, symbols):
        try:
            timeframes = {
                "1m": "1",
                "5m": "5",
                "15m": "15",
                "30m": "30",
                "1hr": "60",
                "2hr": "120",
                "4hr": "240",
                "1D": "",
                "1W": "1W",
                "1M": "1M",
            }
            symbol = self.__format_symbol(symbols.upper(), exchange)
            all_result = []

            for label, tf in timeframes.items():
                fields = self.build_fields_param(tf)
                print(fields)
                params = {
                    "symbol": symbol,
                    "fields": fields,
                    "no_404": 'true',
                    "label-product": 'popup-technicals'
                }

                response = requests.get(url=self.technicals_url, params=params, headers=self.headers).json()
                row = {}
                for key, value in response.items():
                    if tf == "":
                        if "|" not in key:
                            row[key] = value
                    else:
                        if key.endswith(f"|{tf}"):
                            clean_key = key.replace(f"|{tf}", "")
                            row[clean_key] = value



                row['interval'] = label
                all_result.append(row)

            final_df = pd.DataFrame(all_result).set_index("interval")
            return final_df.to_csv("technicals.csv")

        except Exception as e:
            return e

    def get_documents(self, exchange, symbol):
        try:
            symbol = self.__format_symbol(exchange=exchange, symbol=symbol.upper())
            params = {
                "filter": [
                    'category:all',
                    'lang:en',
                    f'symbol: {symbol}'
                ],
                "client": "landing"
            }
            response = requests.get(url=self.docs_url, headers=self.headers, params=params)
            data = response.json()['items']
            return data

        except Exception as e:
            return e


if __name__ == "__main__":

    Symbols = []

    Symbols = ['reliance']

    logging.basicConfig(level=logging.DEBUG)

    tv = TvDatafeed()

    for symbol in Symbols:
        # print(tv.get_quote(symbol, "NSE", True))
        # data = tv.get_quote(symbol, "NSE", True)
        # print(data)
        # print(tv.build_forcast_df(data).to_string())
        # print(tv.build_forcast_df(data).to_csv("forcast.csv", index=False))
        # print(tv.parse_all_metrics_single_df(data).to_csv("reliance.csv", index=False))
        # reliance = tv.parse_all_metrics_single_df(data)
        # df_unique = reliance.drop_duplicates(subset=['metric'], keep='first')
        # print(df_unique.to_csv("metrics.csv", index=False))
        print(tv.get_news("NSE", symbol))
        # print(tv.technicals("NSE", symbol))
        # print(tv.get_documents("NSE", symbol))
