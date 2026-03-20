"""
data_loader.py  —  Walmart UK Polyglot Persistence Dashboard
=============================================================
All database connections, queries, and cached data-prep logic.

Design notes
------------
* SQL Server  : transactional data  (Orders, OrderLine, Product, Store)
* MongoDB     : review / sentiment  (Customer_reviews)
* NPS / Sentiment are filtered by ProductIDs ordered in the selected period
  (not review_date, which is the same for all reviews in this dataset).
* Region and Category filters are applied to all SQL queries when provided.
"""

import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

# ── Path bootstrap ─────────────────────────────────────────────────────────
BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE / "01_Setup"))
sys.path.insert(0, str(BASE / "04_SQL_Server"))
sys.path.insert(0, str(BASE / "05_MongoDB"))

from config import SQL_SERVER_CONFIG  # noqa: E402

_DB = SQL_SERVER_CONFIG.get("database", "WalmartUK")

# ── Optional driver imports ────────────────────────────────────────────────
try:
    from sql_utils import get_sql_connection as _sql_fn
    _SQL_AVAIL = True
except ImportError as _e:
    print(f"[Loader] sql_utils unavailable: {_e}")
    _SQL_AVAIL = False
    _sql_fn = None

try:
    from mongo_utils import get_mongo_connection as _mongo_fn
    _MONGO_AVAIL = True
except ImportError as _e:
    print(f"[Loader] mongo_utils unavailable: {_e}")
    _MONGO_AVAIL = False
    _mongo_fn = None

# ── Lazy singleton connections ─────────────────────────────────────────────
_sql_conn  = None
_mongo_db  = None


def _get_sql():
    global _sql_conn
    if not _SQL_AVAIL:
        return None
    if _sql_conn is not None:
        return _sql_conn
    for trusted in (True, False):
        try:
            _sql_conn = _sql_fn(use_trusted=trusted)
            print(f"[Loader] SQL Server connected (trusted={trusted})")
            return _sql_conn
        except Exception as exc:
            print(f"[Loader] SQL (trusted={trusted}): {exc}")
    return None


def _reset_sql():
    global _sql_conn
    _sql_conn = None


def _get_mongo():
    global _mongo_db
    if not _MONGO_AVAIL:
        return None
    if _mongo_db is not None:
        try:
            _mongo_db.client.admin.command("ping")
            return _mongo_db
        except Exception:
            _mongo_db = None
    try:
        _mongo_db = _mongo_fn()
        return _mongo_db
    except Exception as exc:
        print(f"[Loader] MongoDB: {exc}")
        return None


def connection_status() -> dict:
    return {"sql": _get_sql() is not None, "mongo": _get_mongo() is not None}


# ── SQL helpers ────────────────────────────────────────────────────────────

def _t(name: str) -> str:
    """Fully-qualified three-part table name."""
    return f"[{_DB}].[dbo].[{name}]"


def _sql(query: str, params=None) -> pd.DataFrame:
    """Execute SQL and return a DataFrame; return empty DF on any error."""
    conn = _get_sql()
    if conn is None:
        return pd.DataFrame()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(query, params or [])
        cols = [c[0] for c in cur.description]
        df   = pd.DataFrame([list(r) for r in cur.fetchall()], columns=cols)
        cur.close()
        # Coerce numeric-looking columns (pyodbc returns Decimal for MONEY)
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass  # leave non-numeric columns unchanged
        return df
    except Exception as exc:
        print(f"[Loader] SQL error: {exc}")
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        _reset_sql()
        return pd.DataFrame()


# ── In-memory TTL cache ────────────────────────────────────────────────────
_CACHE: dict = {}
_TTL = 180  # seconds


def _ck(name: str, start: datetime = None, end: datetime = None,
        regions=None, categories=None) -> str:
    s = start.strftime("%Y%m%d") if start else ""
    e = end.strftime("%Y%m%d")   if end   else ""
    r = "|".join(sorted(regions))    if regions    else ""
    c = "|".join(sorted(categories)) if categories else ""
    return f"{name}|{s}|{e}|{r}|{c}"


def _cget(key: str):
    entry = _CACHE.get(key)
    if not entry:
        return None
    ts, val = entry
    if time.time() - ts > _TTL:
        _CACHE.pop(key, None)
        return None
    return val.copy() if isinstance(val, pd.DataFrame) else val


def _cset(key: str, val):
    _CACHE[key] = (time.time(), val.copy() if isinstance(val, pd.DataFrame) else val)


# ── SQL filter fragment helpers ────────────────────────────────────────────
# Each returns (where_snippet, params_list).
# Snippets start with " AND " so they concatenate cleanly after the base WHERE.

def _rgn_frag(regions) -> tuple:
    """Region filter via o.StoreID subquery — no Store join required in caller."""
    if not regions:
        return "", []
    ph = ",".join(["?"] * len(regions))
    return (
        f" AND o.StoreID IN (SELECT StoreID FROM {_t('Store')} WHERE Region IN ({ph}))",
        list(regions),
    )


def _rgn_frag_s(regions) -> tuple:
    """Region filter when Store 's' is already joined in the caller query."""
    if not regions:
        return "", []
    ph = ",".join(["?"] * len(regions))
    return f" AND s.Region IN ({ph})", list(regions)


def _cat_frag_ol(categories) -> tuple:
    """Category filter when OrderLine 'ol' is already joined in the caller query."""
    if not categories:
        return "", []
    ph = ",".join(["?"] * len(categories))
    return (
        f" AND ol.ProductID IN "
        f"(SELECT ProductID FROM {_t('Product')} WHERE Category IN ({ph}))",
        list(categories),
    )


def _cat_frag_p(categories) -> tuple:
    """Category filter when Product 'p' is already joined in the caller query."""
    if not categories:
        return "", []
    ph = ",".join(["?"] * len(categories))
    return f" AND p.Category IN ({ph})", list(categories)


def _cat_frag_orders(categories) -> tuple:
    """Category filter when only Orders 'o' is in the query (no OrderLine/Product joins)."""
    if not categories:
        return "", []
    ph = ",".join(["?"] * len(categories))
    return (
        f" AND o.OrderID IN ("
        f"SELECT DISTINCT ol2.OrderID FROM {_t('OrderLine')} ol2 "
        f"JOIN {_t('Product')} p2 ON p2.ProductID = ol2.ProductID "
        f"WHERE p2.Category IN ({ph}))",
        list(categories),
    )


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def get_date_range() -> tuple:
    """Return (min_date, max_date) from Orders table."""
    df = _sql(f"SELECT MIN(OrderDate) AS mn, MAX(OrderDate) AS mx FROM {_t('Orders')}")
    if df.empty or pd.isna(df["mn"].iloc[0]):
        return datetime(2024, 1, 1), datetime(2026, 3, 31)
    return (
        pd.Timestamp(df["mn"].iloc[0]).to_pydatetime(),
        pd.Timestamp(df["mx"].iloc[0]).to_pydatetime(),
    )


def get_filter_options() -> dict:
    """Return distinct regions and categories for populating filter dropdowns."""
    key = "filter_opts"
    cached = _cget(key)
    if cached is not None:
        return cached
    regions_df = _sql(f"SELECT DISTINCT Region   FROM {_t('Store')}   ORDER BY Region")
    cats_df    = _sql(f"SELECT DISTINCT Category FROM {_t('Product')} ORDER BY Category")
    result = {
        "regions":    regions_df["Region"].tolist()    if not regions_df.empty else [],
        "categories": cats_df["Category"].tolist()     if not cats_df.empty    else [],
    }
    _cset(key, result)
    return result


# ── KPI Metrics ───────────────────────────────────────────────────────────

def get_kpi_data(start: datetime, end: datetime,
                 regions=None, categories=None) -> dict:
    """
    Return KPI dict for current period + comparison (previous equal-length period).

    Revenue  = SUM(OrderLine.LineTotal)   — most accurate line-item total
    Basket   = AVG(Orders.TotalAmount)    — average per-transaction spend
    Repeat % = customers with ≥2 orders / total customers × 100
    NPS      = computed from reviews of products ordered in the period
    """
    key = _ck("kpi", start, end, regions, categories)
    cached = _cget(key)
    if cached is not None:
        return cached

    period_days = max((end - start).days, 1)
    p0 = start - timedelta(days=period_days)
    p1 = start - timedelta(days=1)

    rgn_w,    rgn_p    = _rgn_frag(regions)
    cat_w_ol, cat_p_ol = _cat_frag_ol(categories)      # for rev_q  (has ol)
    cat_w_o,  cat_p_o  = _cat_frag_orders(categories)  # for basket/repeat (no ol)

    _rev_q = f"""
        SELECT ISNULL(SUM(ol.LineTotal), 0) AS revenue
        FROM   {_t('OrderLine')} ol
        JOIN   {_t('Orders')}    o  ON o.OrderID = ol.OrderID
        WHERE  o.OrderDate >= ? AND o.OrderDate <= ?
        {rgn_w}{cat_w_ol}
    """
    _basket_q = f"""
        SELECT
            ISNULL(AVG(o.TotalAmount), 0)      AS avg_basket,
            COUNT(DISTINCT o.OrderID)          AS num_orders
        FROM {_t('Orders')} o
        WHERE o.OrderDate >= ? AND o.OrderDate <= ?
        {rgn_w}{cat_w_o}
    """
    _repeat_q = f"""
        SELECT
            ISNULL(
                COUNT(DISTINCT CASE WHEN ord_cnt >= 2 THEN CustomerID END) * 100.0
                / NULLIF(COUNT(DISTINCT CustomerID), 0),
            0) AS repeat_rate
        FROM (
            SELECT CustomerID, COUNT(*) AS ord_cnt
            FROM   {_t('Orders')} o
            WHERE  o.OrderDate >= ? AND o.OrderDate <= ?
            {rgn_w}{cat_w_o}
            GROUP  BY CustomerID
        ) x
    """

    def _f(df: pd.DataFrame, col: str, default=0.0) -> float:
        if df.empty or col not in df.columns:
            return default
        try:
            v = float(df[col].iloc[0])
            return default if pd.isna(v) else v
        except Exception:
            return default

    base_ol = [start, end] + rgn_p + cat_p_ol
    base_o  = [start, end] + rgn_p + cat_p_o
    prev_ol = [p0, p1]     + rgn_p + cat_p_ol
    prev_o  = [p0, p1]     + rgn_p + cat_p_o

    cur_rev  = _sql(_rev_q,    base_ol);  prev_rev  = _sql(_rev_q,    prev_ol)
    cur_bas  = _sql(_basket_q, base_o);   prev_bas  = _sql(_basket_q, prev_o)
    cur_rep  = _sql(_repeat_q, base_o);   prev_rep  = _sql(_repeat_q, prev_o)

    # NPS filtered by products ordered in each period (respects region/category)
    cur_pids  = _get_product_ids_in_period(start, end, regions, categories)
    prev_pids = _get_product_ids_in_period(p0, p1,     regions, categories)
    cur_nps   = _nps_for_products(cur_pids)
    prev_nps  = _nps_for_products(prev_pids)

    result = {
        "revenue":      _f(cur_rev,  "revenue"),
        "prev_revenue": _f(prev_rev, "revenue"),
        "avg_basket":   _f(cur_bas,  "avg_basket"),
        "prev_basket":  _f(prev_bas, "avg_basket"),
        "num_orders":   _f(cur_bas,  "num_orders"),
        "repeat_rate":  _f(cur_rep,  "repeat_rate"),
        "prev_repeat":  _f(prev_rep, "repeat_rate"),
        "nps":          cur_nps,
        "prev_nps":     prev_nps,
    }
    _cset(key, result)
    return result


def _get_product_ids_in_period(start: datetime, end: datetime,
                                regions=None, categories=None) -> list:
    """
    Return distinct ProductIDs that had at least one order in [start, end],
    optionally filtered by region and/or category.
    Used to make NPS and sentiment genuinely period-sensitive.
    """
    key = _ck("pids", start, end, regions, categories)
    cached = _cget(key)
    if cached is not None:
        return cached

    rgn_w,    rgn_p    = _rgn_frag(regions)
    cat_w_ol, cat_p_ol = _cat_frag_ol(categories)

    df = _sql(
        f"""SELECT DISTINCT ol.ProductID
            FROM {_t('OrderLine')} ol
            JOIN {_t('Orders')}    o ON o.OrderID = ol.OrderID
            WHERE o.OrderDate >= ? AND o.OrderDate <= ?
            {rgn_w}{cat_w_ol}""",
        [start, end] + rgn_p + cat_p_ol,
    )
    ids = df["ProductID"].tolist() if not df.empty else []
    _cset(key, ids)
    return ids


def _nps_for_products(product_ids: list) -> int:
    """
    NPS calculated from reviews belonging to the given ProductIDs.
    Returns 0 when MongoDB is unavailable or no matching reviews exist.
    NOTE: MongoDB stores ProductID as int — do NOT convert to str.
    """
    if not product_ids:
        return 0
    db = _get_mongo()
    if db is None:
        return 0
    try:
        pipeline = [
            {"$match": {"ProductID": {"$in": [int(p) for p in product_ids]}}},
            {"$group": {
                "_id":        None,
                "total":      {"$sum": 1},
                "promoters":  {"$sum": {"$cond": [{"$gte": ["$rating", 4]}, 1, 0]}},
                "detractors": {"$sum": {"$cond": [{"$lte": ["$rating", 2]}, 1, 0]}},
            }},
            {"$project": {
                "nps": {"$round": [
                    {"$subtract": [
                        {"$multiply": [{"$divide": ["$promoters",  "$total"]}, 100]},
                        {"$multiply": [{"$divide": ["$detractors", "$total"]}, 100]},
                    ]}, 0
                ]}
            }},
        ]
        r = list(db["Customer_reviews"].aggregate(pipeline))
        return int(r[0]["nps"]) if r else 0
    except Exception as exc:
        print(f"[Loader] NPS: {exc}")
        return 0


# ── Sparklines ────────────────────────────────────────────────────────────

def get_sparkline_data(start: datetime, end: datetime,
                       regions=None, categories=None) -> pd.DataFrame:
    """Monthly revenue, avg-basket, and order count for KPI sparklines."""
    key = _ck("spark", start, end, regions, categories)
    cached = _cget(key)
    if cached is not None:
        return cached

    rgn_w,   rgn_p   = _rgn_frag(regions)
    cat_w_o, cat_p_o = _cat_frag_orders(categories)

    q = f"""
        SELECT
            YEAR(o.OrderDate)  AS yr,
            MONTH(o.OrderDate) AS mo,
            ISNULL(SUM(o.TotalAmount), 0)   AS revenue,
            ISNULL(AVG(o.TotalAmount), 0)   AS avg_basket,
            COUNT(DISTINCT o.OrderID)       AS num_orders
        FROM {_t('Orders')} o
        WHERE o.OrderDate >= ? AND o.OrderDate <= ?
        {rgn_w}{cat_w_o}
        GROUP BY YEAR(o.OrderDate), MONTH(o.OrderDate)
        ORDER BY yr, mo
    """
    df = _sql(q, [start, end] + rgn_p + cat_p_o)
    _cset(key, df)
    return df


# ── Product Performance (bubble scatter) ──────────────────────────────────

def get_product_performance(start: datetime, end: datetime,
                             regions=None, categories=None) -> pd.DataFrame:
    """
    Per-product: SQL revenue/orders + MongoDB avg rating & review count.
    Used for the Product Performance bubble chart.
    Ratings are ALL-TIME (no date filter on MongoDB).
    """
    key = _ck("prod", start, end, regions, categories)
    cached = _cget(key)
    if cached is not None:
        return cached

    rgn_w,   rgn_p   = _rgn_frag(regions)
    cat_w_p, cat_p_p = _cat_frag_p(categories)   # Product 'p' is already joined

    q = f"""
        SELECT
            p.ProductID,
            p.ProductName,
            p.Category,
            ISNULL(SUM(ol.LineTotal), 0)  AS revenue,
            COUNT(DISTINCT o.OrderID)      AS num_orders
        FROM {_t('Product')}   p
        JOIN {_t('OrderLine')} ol ON ol.ProductID = p.ProductID
        JOIN {_t('Orders')}    o  ON o.OrderID    = ol.OrderID
        WHERE o.OrderDate >= ? AND o.OrderDate <= ?
        {rgn_w}{cat_w_p}
        GROUP BY p.ProductID, p.ProductName, p.Category
    """
    df = _sql(q, [start, end] + rgn_p + cat_p_p)
    if df.empty:
        _cset(key, df)
        return df

    db = _get_mongo()
    if db is not None:
        try:
            ratings = list(db["Customer_reviews"].aggregate([
                {"$group": {
                    "_id":         "$ProductID",
                    "avg_rating":  {"$avg": "$rating"},
                    "num_reviews": {"$sum": 1},
                }}
            ]))
            if ratings:
                r_df = (
                    pd.DataFrame(ratings)
                    .rename(columns={"_id": "ProductID"})
                )
                r_df["ProductID"] = pd.to_numeric(r_df["ProductID"], errors="coerce")
                df["ProductID"]   = pd.to_numeric(df["ProductID"],   errors="coerce")
                df = df.merge(r_df, on="ProductID", how="left")
        except Exception as exc:
            print(f"[Loader] Product ratings: {exc}")

    df["avg_rating"]  = pd.to_numeric(df.get("avg_rating",  None), errors="coerce").fillna(3.0)
    df["num_reviews"] = pd.to_numeric(df.get("num_reviews", None), errors="coerce").fillna(0).astype(int)
    _cset(key, df)
    return df


# ── Individual Reviews + Revenue (legacy — not used in active dashboard) ──

def get_reviews_with_revenue(start: datetime, end: datetime) -> pd.DataFrame:
    """Each row = ONE individual MongoDB review, joined to its product's SQL revenue."""
    key = _ck("rev_corr", start, end)
    cached = _cget(key)
    if cached is not None:
        return cached

    prod_df = get_product_performance(start, end)
    if prod_df.empty:
        _cset(key, pd.DataFrame())
        return pd.DataFrame()

    db = _get_mongo()
    if db is None:
        _cset(key, pd.DataFrame())
        return pd.DataFrame()

    try:
        reviews = list(
            db["Customer_reviews"].find(
                {}, {"ProductID": 1, "rating": 1, "sentiment": 1, "_id": 0}
            )
        )
        if not reviews:
            _cset(key, pd.DataFrame())
            return pd.DataFrame()

        r_df = pd.DataFrame(reviews)
        r_df["ProductID"] = pd.to_numeric(r_df["ProductID"], errors="coerce")
        prod_df = prod_df.copy()
        prod_df["ProductID"] = pd.to_numeric(prod_df["ProductID"], errors="coerce")

        merged = r_df.merge(
            prod_df[["ProductID", "ProductName", "Category", "revenue"]],
            on="ProductID",
            how="inner",
        )
        if "sentiment" in merged.columns:
            merged["sentiment"] = merged["sentiment"].str.capitalize()

        _cset(key, merged)
        return merged
    except Exception as exc:
        print(f"[Loader] Reviews correlation: {exc}")
        _cset(key, pd.DataFrame())
        return pd.DataFrame()


# ── Category Performance (scorecard) ─────────────────────────────────────

def get_category_performance(start: datetime, end: datetime,
                              regions=None, categories=None) -> pd.DataFrame:
    """
    Category-level aggregation built on top of get_product_performance().

    Columns: Category, revenue, num_orders, avg_basket, avg_rating, num_reviews, rev_pct
    """
    key = _ck("category", start, end, regions, categories)
    cached = _cget(key)
    if cached is not None:
        return cached

    prod = get_product_performance(start, end, regions, categories)
    if prod.empty:
        _cset(key, pd.DataFrame())
        return pd.DataFrame()

    cat = (
        prod.groupby("Category", as_index=False)
        .agg(
            revenue    =("revenue",     "sum"),
            num_orders =("num_orders",  "sum"),
            avg_rating =("avg_rating",  "mean"),
            num_reviews=("num_reviews", "sum"),
        )
        .sort_values("revenue", ascending=False)
    )
    cat["avg_basket"] = cat["revenue"] / cat["num_orders"].clip(lower=1)
    total = cat["revenue"].sum()
    cat["rev_pct"] = (cat["revenue"] / total * 100).round(1) if total > 0 else 0.0

    _cset(key, cat)
    return cat


# ── Regional Performance ──────────────────────────────────────────────────

def get_regional_performance(start: datetime, end: datetime,
                              regions=None, categories=None) -> pd.DataFrame:
    key = _ck("regional", start, end, regions, categories)
    cached = _cget(key)
    if cached is not None:
        return cached

    rgn_w_s, rgn_p_s = _rgn_frag_s(regions)       # Store 's' is already joined
    cat_w_o, cat_p_o = _cat_frag_orders(categories)

    q = f"""
        SELECT
            s.Region,
            ISNULL(SUM(o.TotalAmount), 0)  AS revenue,
            COUNT(DISTINCT o.OrderID)       AS num_orders,
            COUNT(DISTINCT o.CustomerID)    AS num_customers,
            ISNULL(AVG(o.TotalAmount), 0)   AS avg_order
        FROM {_t('Orders')} o
        JOIN {_t('Store')}  s ON s.StoreID = o.StoreID
        WHERE o.OrderDate >= ? AND o.OrderDate <= ?
        {rgn_w_s}{cat_w_o}
        GROUP BY s.Region
        ORDER BY revenue DESC
    """
    df = _sql(q, [start, end] + rgn_p_s + cat_p_o)
    _cset(key, df)
    return df


# ── Sentiment — filtered by products ordered in period ────────────────────

def get_sentiment_data(start: datetime, end: datetime,
                       regions=None, categories=None) -> pd.DataFrame:
    """
    Sentiment breakdown for reviews of products ordered in [start, end],
    optionally filtered by region and/or category.

    Why not filter by review_date?  All 257 reviews share the same generation
    date (2026-03-02), so date-filtering yields nothing useful.  Instead we
    filter by ProductIDs that had orders in the selected period.
    """
    key = _ck("sentiment", start, end, regions, categories)
    cached = _cget(key)
    if cached is not None:
        return cached

    fallback = pd.DataFrame({
        "sentiment": ["Positive", "Neutral", "Negative"],
        "count":     [0, 0, 0],
    })
    db = _get_mongo()
    if db is None:
        return fallback

    product_ids = _get_product_ids_in_period(start, end, regions, categories)
    if not product_ids:
        _cset(key, fallback)
        return fallback

    try:
        pipeline = [
            {"$match": {"ProductID": {"$in": [int(p) for p in product_ids]}}},
            {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
        res = list(db["Customer_reviews"].aggregate(pipeline))
        if not res:
            _cset(key, fallback)
            return fallback
        df = pd.DataFrame(res).rename(columns={"_id": "sentiment"})
        df["sentiment"] = df["sentiment"].str.capitalize()
        _cset(key, df)
        return df
    except Exception as exc:
        print(f"[Loader] Sentiment: {exc}")
        return fallback


# ── Revenue + Rating Trend (dual-axis) ───────────────────────────────────

def get_trend_data(start: datetime, end: datetime,
                   regions=None, categories=None) -> pd.DataFrame:
    """
    Monthly revenue from SQL (OrderLine.LineTotal).
    Avg rating is ALL-TIME from MongoDB (shown as a constant reference line).
    """
    key = _ck("trend", start, end, regions, categories)
    cached = _cget(key)
    if cached is not None:
        return cached

    rgn_w,    rgn_p    = _rgn_frag(regions)
    cat_w_ol, cat_p_ol = _cat_frag_ol(categories)

    q = f"""
        SELECT
            YEAR(o.OrderDate)  AS yr,
            MONTH(o.OrderDate) AS mo,
            ISNULL(SUM(ol.LineTotal), 0) AS revenue,
            COUNT(DISTINCT o.OrderID)    AS num_orders
        FROM {_t('Orders')}    o
        JOIN {_t('OrderLine')} ol ON ol.OrderID = o.OrderID
        WHERE o.OrderDate >= ? AND o.OrderDate <= ?
        {rgn_w}{cat_w_ol}
        GROUP BY YEAR(o.OrderDate), MONTH(o.OrderDate)
        ORDER BY yr, mo
    """
    df = _sql(q, [start, end] + rgn_p + cat_p_ol)
    if df.empty:
        _cset(key, df)
        return df

    df["date"] = pd.to_datetime(
        df["yr"].astype(str) + "-" + df["mo"].astype(str).str.zfill(2) + "-01"
    )

    # All-time avg rating (constant reference line — not filtered)
    avg_rating = None
    db = _get_mongo()
    if db is not None:
        try:
            r = list(db["Customer_reviews"].aggregate([
                {"$group": {"_id": None, "avg": {"$avg": "$rating"}}}
            ]))
            if r and r[0].get("avg") is not None:
                avg_rating = round(float(r[0]["avg"]), 2)
        except Exception as exc:
            print(f"[Loader] Trend rating: {exc}")

    df["avg_rating"] = avg_rating
    _cset(key, df)
    return df
