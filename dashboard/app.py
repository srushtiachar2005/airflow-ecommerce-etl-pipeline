import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.express as px
from dotenv import load_dotenv
import os

load_dotenv()

st.set_page_config(page_title="Palmonas Dashboard", layout="wide")
client = MongoClient(os.getenv("MONGO_URI"))
# ------------------- CONFIG -------------------

db = client["palmonas_db"]
collection = db["products"]

data = list(collection.find())

if not data:
    st.error("No data found")
    st.stop()

df = pd.DataFrame(data)

# ------------------- HEADER -------------------
st.title("🛍️ Palmonas Product Analytics Dashboard")

# ------------------- SIDEBAR FILTERS -------------------
st.sidebar.header("🔍 Filters")

category_filter = st.sidebar.multiselect(
    "Select Category",
    options=df["category"].unique(),
    default=df["category"].unique()
)

price_range = st.sidebar.slider(
    "Price Range",
    int(df["price"].min()),
    int(df["price"].max()),
    (int(df["price"].min()), int(df["price"].max()))
)

df = df[
    (df["category"].isin(category_filter)) &
    (df["price"] >= price_range[0]) &
    (df["price"] <= price_range[1])
]

# ------------------- KPI SECTION -------------------
st.subheader("📊 Key Metrics")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Products", len(df))
col2.metric("Avg Price", round(df["price"].mean(), 2))
col3.metric("Avg Discount %", round(df["discount_percent"].mean(), 2))
col4.metric("Max Price", df["price"].max())

# ------------------- CHARTS -------------------
st.subheader("📈 Analytics")

col1, col2 = st.columns(2)

# Category chart
cat_data = df["category"].value_counts().reset_index()
cat_data.columns = ["Category", "Count"]

fig1 = px.bar(cat_data, x="Category", y="Count", title="Products by Category")
col1.plotly_chart(fig1, use_container_width=True)

# Price distribution
fig2 = px.histogram(df, x="price", nbins=30, title="Price Distribution")
col2.plotly_chart(fig2, use_container_width=True)

# ------------------- PRODUCT GRID -------------------
st.subheader("🛍️ Product Showcase")

cols = st.columns(4)

for i, row in df.head(20).iterrows():
    col = cols[i % 4]

    with col:
        st.image(row["images"][0] if row["images"] else "", use_column_width=True)
        st.markdown(f"**{row['title']}**")
        st.markdown(f"💰 ₹{row['price']}")
        st.markdown(f"📉 {row['discount_percent']}% OFF")

# ------------------- TOP PRODUCTS -------------------
st.subheader("🔥 Top Discount Products")

top = df.sort_values(by="discount_percent", ascending=False).head(10)

st.dataframe(top[["title", "price", "mrp", "discount_percent"]])
st.subheader("🔥 Fast Selling Products (Estimated)")

df = df.sort_values(["product_id", "scrape_date"])

dates = sorted(df["scrape_date"].unique())

if len(dates) >= 2:
    day1 = df[df["scrape_date"] == dates[-2]]
    day2 = df[df["scrape_date"] == dates[-1]]

    sold_products = set(day1["product_id"]) - set(day2["product_id"])

    fast_df = df[df["product_id"].isin(sold_products)]

    st.dataframe(fast_df[["title", "price", "discount_percent"]].drop_duplicates())