import streamlit as st
from dotenv import load_dotenv
import os
import snowflake.connector
import pandas as pd
import altair as alt
from datetime import date

load_dotenv()


def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

TS = "TO_TIMESTAMP(o.created_at / 1000000000)"


@st.cache_data
def get_kpis():
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute(f"""
        WITH monthly AS (
            SELECT
                DATE_TRUNC('month', {TS})::DATE                        AS month,
                SUM(oi.price_usd)                                       AS revenue,
                COUNT(DISTINCT o.order_id)                              AS orders,
                COUNT(oi.order_item_id)                                 AS items,
                SUM(oi.price_usd) / NULLIF(COUNT(DISTINCT o.order_id), 0) AS aov
            FROM raw.order_items oi
            JOIN raw.orders o ON oi.order_id = o.order_id
            GROUP BY 1
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (ORDER BY month DESC) AS rn
            FROM monthly
        )
        SELECT
            cur.revenue, cur.orders, cur.items, cur.aov,
            prior.revenue, prior.orders, prior.items, prior.aov
        FROM ranked cur
        LEFT JOIN ranked prior ON prior.rn = cur.rn + 1
        WHERE cur.rn = 1
    """)
    row = cur.fetchone()
    conn.close()
    return row


@st.cache_data
def get_date_bounds():
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            TO_DATE(TO_TIMESTAMP(MIN(created_at) / 1000000000)),
            TO_DATE(TO_TIMESTAMP(MAX(created_at) / 1000000000))
        FROM raw.orders
    """)
    row = cur.fetchone()
    conn.close()
    _min, _max = row
    to_date = lambda v: v if isinstance(v, date) else date.fromisoformat(str(v)[:10])
    return to_date(_min), to_date(_max)


@st.cache_data
def get_product_names():
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("SELECT product_name FROM raw.products ORDER BY product_name")
    names = [r[0] for r in cur.fetchall()]
    conn.close()
    return names


@st.cache_data
def get_bundle_pairs(product_name):
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            p2.product_name   AS also_bought,
            COUNT(DISTINCT a.order_id) AS order_count
        FROM raw.order_items a
        JOIN raw.products p1 ON a.product_id = p1.product_id
        JOIN raw.order_items b  ON a.order_id = b.order_id AND a.order_item_id <> b.order_item_id
        JOIN raw.products p2 ON b.product_id = p2.product_id
        WHERE p1.product_name = %s
          AND p2.product_name <> %s
        GROUP BY 1
        ORDER BY 2 DESC
    """, (product_name, product_name))
    rows = cur.fetchall()
    conn.close()
    return pd.DataFrame(rows, columns=["Also bought", "# of orders"])


@st.cache_data
def get_top_products(start, end):
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            p.product_name,
            SUM(oi.price_usd) AS revenue
        FROM raw.order_items oi
        JOIN raw.orders o  ON oi.order_id  = o.order_id
        JOIN raw.products p ON oi.product_id = p.product_id
        WHERE TO_DATE({TS}) BETWEEN %s AND %s
        GROUP BY 1
        ORDER BY 2 DESC
    """, (start, end))
    rows = cur.fetchall()
    conn.close()
    return pd.DataFrame(rows, columns=["Product", "Revenue"])


@st.cache_data
def get_revenue_trend(start, end):
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            TO_DATE({TS}) AS day,
            SUM(oi.price_usd) AS revenue
        FROM raw.order_items oi
        JOIN raw.orders o ON oi.order_id = o.order_id
        WHERE TO_DATE({TS}) BETWEEN %s AND %s
        GROUP BY 1
        ORDER BY 1
    """, (start, end))
    rows = cur.fetchall()
    conn.close()
    return pd.DataFrame(rows, columns=["Day", "Revenue"])


# ── KPIs ─────────────────────────────────────────────────────────────
st.title("Basket Craft — Merchandising Dashboard")

row = get_kpis()
revenue, orders, items, aov, p_revenue, p_orders, p_items, p_aov = row

def abs_delta(cur, prior, prefix=""):
    if prior is None:
        return None
    diff = cur - prior
    sign = "+" if diff >= 0 else "-"
    return f"{sign}{prefix}{abs(diff):,.2f}" if prefix else f"{sign}{abs(diff):,.0f}"

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total revenue",      f"${revenue:,.0f}", abs_delta(revenue, p_revenue, "$"))
c2.metric("Total orders",       f"{orders:,}",      abs_delta(orders,  p_orders))
c3.metric("Average order value",f"${aov:,.2f}",     abs_delta(aov,     p_aov, "$"))
c4.metric("Total items sold",   f"{items:,}",       abs_delta(items,   p_items))

# ── Date range filter ────────────────────────────────────────────────
min_date, max_date = get_date_bounds()

st.subheader("Revenue trend")
date_range = st.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

# ── Trend chart ───────────────────────────────────────────────────────
df_trend = get_revenue_trend(start_date, end_date)
st.line_chart(df_trend.set_index("Day")["Revenue"])

days = (end_date - start_date).days + 1
total = df_trend["Revenue"].sum()
daily_avg = total / days if days > 0 else 0
st.caption(f"{days} days · total ${total:,.0f} · daily avg ${daily_avg:,.0f}")

# ── Top products bar chart ────────────────────────────────────────────
st.subheader("Top products by revenue")
df_products = get_top_products(start_date, end_date)
chart = alt.Chart(df_products).mark_bar().encode(
    x=alt.X("Revenue:Q", title="Revenue ($)"),
    y=alt.Y("Product:N", sort="-x", title=None),
)
st.altair_chart(chart, use_container_width=True)

# ── Bundle finder ─────────────────────────────────────────────────────
st.subheader("Bundle Finder: Bought With…")
product_names = get_product_names()
selected = st.selectbox("Pick a product", product_names)
df_bundles = get_bundle_pairs(selected)
st.dataframe(df_bundles, use_container_width=True, hide_index=True)
st.download_button(
    label="Download CSV",
    data=df_bundles.to_csv(index=False),
    file_name=f"bundles_{selected.replace(' ', '_')}.csv",
    mime="text/csv",
)
