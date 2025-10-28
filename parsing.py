import re
from sqlalchemy import text
from db.db_ops import engine

# 1️⃣ --- Query Parser ---
def parse_query_with_period(user_query: str):
    """
    Parses queries like:
      "ROE > 15"
      "Sales_Q > 10000"
      "OPM_quarterly > 10"
    Returns:
      list of dicts: [{'metric_name': 'ROE', 'operator': '>', 'value': 15, 'period': 'A'}, ...]
    """
    pattern = r'([A-Za-z0-9_ ]+)\s*(>=|<=|=|>|<)\s*([\d\.]+)'
    matches = re.findall(pattern, user_query)
    parsed = []

    for metric_raw, op, val in matches:
        metric_lower = metric_raw.lower().strip()
        metric_lower = re.sub(r'^(and|or)\s+', '', metric_lower).strip()

        # detect if user explicitly requested quarterly data (suffix-based or word-based)
        if metric_lower.endswith("_q") or metric_lower.endswith("_quarter") or metric_lower.endswith("_quarterly"):
            period = "Q"
            metric_name = re.sub(r'(_q|_quarter|_quarterly)$', '', metric_lower).strip()
        elif metric_lower.startswith("quarterly "):  # if metric name starts with 'quarterly', keep it
            period = "Q"
            metric_name = metric_lower.strip()  # keep full name like 'quarterly opm'
        else:
            period = "A"
            metric_name = metric_lower.strip()

        parsed.append({
            "metric_name": metric_name,
            "operator": op,
            "value": float(val),
            "period": period
        })

    return parsed

# 2️⃣ --- SQL to Evaluate One Condition ---
def screen_single_condition(engine, cond):
    sql = """
    WITH Ranked AS (
        SELECT
            symbol,
            metric_value,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY "timestamp" DESC) AS rn
        FROM public.stock_data
        WHERE
            LOWER(metric_name) = LOWER(:metric)
            AND UPPER(period_code) LIKE :period
            AND "timestamp" >= (CURRENT_DATE - INTERVAL '2 year')
    )
    SELECT symbol
    FROM Ranked
    WHERE rn = 1 AND metric_value {op} :value
    """
    period_like = 'A%' if cond['period'] == 'A' else 'Q%'
    formatted_sql = sql.format(op=cond["operator"])

    with engine.connect() as conn:
        rows = conn.execute(
            text(formatted_sql),
            {
                "metric": cond["metric_name"],
                "period": period_like,
                "value": cond["value"]
            }
        ).fetchall()

    return set(r[0] for r in rows)


# 3️⃣ --- Main Function to Combine All Conditions ---
def screen_stocks(engine, user_query):
    # Normalize the query
    query_upper = user_query.upper()

    # Detect logic automatically
    if " OR " in query_upper:
        logic = "OR"
    else:
        logic = "AND"

    # Parse all conditions
    parsed_conditions = parse_query_with_period(user_query)
    if not parsed_conditions:
        return []

    all_sets = []
    for cond in parsed_conditions:
        symbols = screen_single_condition(engine, cond)
        all_sets.append(symbols)

    if not all_sets:
        return []

    # Combine sets based on detected logic
    if logic == 'AND':
        result = set.intersection(*all_sets)
    else:
        result = set.union(*all_sets)

    return sorted(result)


query_1 = "ROE > 15"                     # uses annual by default
query_2 = "sales_Q > 10000"              # user explicitly wants quarterly
query_3 = "OPM_quarter > 10 and ROE > 15"
query_4 = "ROE > 15 AND (OPM > 10 OR Sales_Q > 5000)"
stocks_1 = screen_stocks(engine, query_1)
stocks_2 = screen_stocks(engine, query_2)
stocks_3 = screen_stocks(engine, query_3)
stocks_4 = screen_stocks(engine, query_4)

print("Annual screen:", stocks_1)
print("Quarterly screen:", stocks_2)
print("Mixed screen:", stocks_3)
print("OPM screen", stocks_4)