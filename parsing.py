import re
from sqlalchemy import text
from db.db_ops import engine


# 1️⃣ --- Query Parser ---
def parse_query_with_period(user_query: str):
    pattern = r'([A-Za-z0-9_ ]+)\s*(>=|<=|=|>|<)\s*([\d\.]+)'
    matches = re.findall(pattern, user_query, flags=re.IGNORECASE)
    parsed = []

    for metric_raw, op, val in matches:
        metric_lower = metric_raw.lower().strip()

        # ✅ Remove any leading logical operators
        metric_lower = re.sub(r'^(and|or)\s+', '', metric_lower).strip()

        # detect if user explicitly requested quarterly data (suffix-based or word-based)
        if metric_lower.endswith(("_q", "_quarter", "_quarterly")):
            period = "Q"
            metric_name = re.sub(r'(_q|_quarter|_quarterly)$', '', metric_lower).strip()
        elif metric_lower.startswith("quarterly "):  # if metric name starts with 'quarterly'
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


def screen_stocks(engine, user_query):
    # normalize boolean words & brackets so we catch them reliably
    q_norm = user_query.upper().replace("&", " AND ").replace("|", " OR ")

    # get parsed conditions (keeps order)
    parsed_conditions = parse_query_with_period(user_query)
    if not parsed_conditions:
        return []

    # find connectors between conditions in order (AND/OR). If none found, default to 'AND'
    connectors = re.findall(r'\b(AND|OR)\b', q_norm)
    # If connectors length doesn't match (n-1), pad or truncate conservatively
    expected = max(0, len(parsed_conditions) - 1)
    if len(connectors) < expected:
        # pad with AND (safe default)
        connectors += ['AND'] * (expected - len(connectors))
    elif len(connectors) > expected:
        connectors = connectors[:expected]

    # Evaluate each condition individually to a set of symbols
    sets = [screen_single_condition(engine, cond) for cond in parsed_conditions]

    # Combine respecting precedence: group by AND sequences, then union groups with OR
    or_groups = []
    if not sets:
        return []

    current_and_set = sets[0]
    for i, conn in enumerate(connectors):
        next_set = sets[i + 1]
        if conn == 'AND':
            # intersection continues the current AND-group
            current_and_set = current_and_set & next_set
        else:  # conn == 'OR'
            # finish current AND-group, append to OR-groups, start a new AND-group
            or_groups.append(current_and_set)
            current_and_set = next_set

    # append last running group
    or_groups.append(current_and_set)

    # final result is union of all OR groups
    if not or_groups:
        result_set = set()
    else:
        result_set = set().union(*or_groups)

    return sorted(result_set)


if __name__ == '__main__':
    query_1 = "10 < ROE < 15"  # uses annual by default
    query_2 = "sales_Q > 10000"  # user explicitly wants quarterly
    query_3 = "OPM > 10 and ROE > 15"
    query_4 = "OPM_quarter > 10 and ROE > 15 or sales_q > 10000"
    query_5 = "sales_q > 10000 or OPM_quarter > 10 and ROE > 15"
    stocks_1 = screen_stocks(engine, query_1)
    stocks_2 = screen_stocks(engine, query_2)
    stocks_3 = screen_stocks(engine, query_3)
    stocks_4 = screen_stocks(engine, query_4)
    stocks_5 = screen_stocks(engine, query_5)

    print("Annual screen:", stocks_1)
    print("Quarterly screen:", stocks_2)
    print("Mixed screen:", stocks_3)
    print("Operator screen", stocks_4)
    print("Operator screen2", stocks_5)
