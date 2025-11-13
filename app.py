from fastapi import FastAPI, HTTPException
from parsing import screen_stocks
from screener import Screener
from db.db_ops import engine

app = FastAPI()
screener_api = Screener()


@app.get("/")
def root():
    return {"message": "Hello World"}


@app.get("/screener/{symbol}")
def screener(symbol: str):
    symbol = symbol.upper()
    file_path = screener_api.fetch_data(symbol)
    if not file_path:
        raise HTTPException(status_code=404, detail=f"No file generated for {symbol}")
    data = screener_api.read_excel(file_path, symbol)
    if data.empty:
        raise HTTPException(status_code=500, detail=f"Something went wrong while fetching data for {symbol}")
    screener_api.dump_ts_to_db(data)
    return {"symbol": symbol, "data": data}


@app.get(
    "/screen",
    summary="Run financial screener query",
    description=(
        "💡 **Hint**:\n"
        "- For quarterly metrics, use suffixes like `_q`, `_quarterly`, or `_quarter`.\n"
        "- Example: `ROE_q > 15 and OPM_quarter > 10`\n"
        "- Use `and` / `or` for combining multiple filters.\n"
    )
         )
def screen(query: str):
    stocks = screen_stocks(engine=engine, user_query=query)
    return {"company": stocks}
