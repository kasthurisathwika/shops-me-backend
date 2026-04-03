import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

DB_USER = "swiggy_user"
DB_PASS = "Swiggy@123"
DB_HOST = "localhost"
DB_NAME = "swiggy_clone"

url = URL.create(
    "mysql+pymysql",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    database=DB_NAME,
    query={"charset": "utf8mb4"},
)

engine = create_engine(url)

def reset_tables():
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        conn.execute(text("TRUNCATE TABLE order_items;"))
        conn.execute(text("TRUNCATE TABLE orders;"))
        conn.execute(text("TRUNCATE TABLE menu_items;"))
        conn.execute(text("TRUNCATE TABLE stores;"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

def nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    return df.where(pd.notnull(df), None)

def keep_only_existing_columns(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    with engine.connect() as conn:
        rows = conn.execute(text(f"SHOW COLUMNS FROM {table_name}")).mappings().all()
        table_cols = [r["Field"] for r in rows]
    keep = [c for c in table_cols if c in df.columns]
    return df[keep]

# ✅ Always start clean during setup/testing
reset_tables()

# -------------------------
# READ EXCEL FILES
# -------------------------
stores = pd.read_excel("stores.xlsx")
menu = pd.read_excel("store_menu.xlsx")
orders = pd.read_excel("orders.xlsx")
order_items = pd.read_excel("order_items.xlsx")

stores = nan_to_none(stores)
menu = nan_to_none(menu)
orders = nan_to_none(orders)
order_items = nan_to_none(order_items)

# -------------------------
# FIX / NORMALIZE COLUMNS
# -------------------------
if "date" in orders.columns:
    orders["order_datetime"] = pd.to_datetime(orders["date"], errors="coerce")
    orders = orders.drop(columns=["date"])

if "store_id" in orders.columns:
    orders["store_id"] = orders["store_id"].replace({0: None})

if "item_id" in menu.columns:
    menu = menu.rename(columns={"item_id": "store_item_id"})

# -------------------------
# KEEP ONLY COLUMNS THAT EXIST IN MYSQL TABLE
# -------------------------
stores = keep_only_existing_columns(stores, "stores")
menu = keep_only_existing_columns(menu, "menu_items")
orders = keep_only_existing_columns(orders, "orders")
order_items = keep_only_existing_columns(order_items, "order_items")

# -------------------------
# INSERT INTO MYSQL
# -------------------------
stores.to_sql("stores", engine, if_exists="append", index=False)
menu.to_sql("menu_items", engine, if_exists="append", index=False)

# ✅ Fix FK: orders.store_id must exist in stores
with engine.connect() as conn:
    existing_store_ids = {
        row[0] for row in conn.execute(text("SELECT store_id FROM stores")).fetchall()
    }

if "store_id" in orders.columns:
    orders["store_id"] = orders["store_id"].apply(
        lambda x: x if (x is None or x in existing_store_ids) else None
    )

orders.to_sql("orders", engine, if_exists="append", index=False)

# ✅ FIX FK: order_items.order_id must exist in orders
with engine.connect() as conn:
    existing_order_ids = {
        row[0] for row in conn.execute(text("SELECT order_id FROM orders")).fetchall()
    }

if "order_id" in order_items.columns:
    before = len(order_items)
    order_items = order_items[order_items["order_id"].isin(existing_order_ids)]
    after = len(order_items)
    print(f"🧹 Filtered order_items: kept {after}/{before} rows (matched existing orders)")

order_items.to_sql("order_items", engine, if_exists="append", index=False)

print("✅ Import finished successfully!")
