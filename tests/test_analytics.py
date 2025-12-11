import pandas as pd
from src.analytics import SalesAnalytics

def sample_data():
    # Fact table
    stg_sales = pd.DataFrame([
        {"transaction_id": 123, "product_id": 3, "location_id": 2, "payment_id": 1, "transaction_date": "2024-01-01", "total_spent": 5.00},
        {"transaction_id": 234, "product_id": 4, "location_id": 2, "payment_id": 2, "transaction_date": "2024-01-01", "total_spent": 7.00},
        {"transaction_id": 345, "product_id": 3, "location_id": 1, "payment_id": 1, "transaction_date": "2024-01-02", "total_spent": 3.00},
    ])


    #Dimension tables
    stg_product = pd.DataFrame([
        {"product_id": 3, "Item": "Coffee"},
        {"product_id": 4, "Item": "Bagel"},
    ])

    stg_location = pd.DataFrame([
        {"location_id": 1, "location_type": "In-Store"},
        {"location_id": 2, "location_type": "Takeaway"},
    ])

    stg_payment = pd.DataFrame([
        {"payment_id": 1, "payment_method": "Cash"},
        {"payment_id": 2, "payment_method": "Card"},
    ])

    return stg_sales, stg_product, stg_location, stg_payment


def test_sales_by_product():
    stg_sales, stg_product, stg_location, stg_payment = sample_data()
    analytics = SalesAnalytics(stg_sales, stg_product, stg_location, stg_payment)

    df = analytics.sales_by_product()

    assert list(df.columns) == ["Item", "total_spent"]
    assert df.iloc[0]["Item"] == "Coffee"   
    assert df.iloc[0]["total_spent"] == 8
    assert df.iloc[1]["Item"] == "Bagel" 
    assert df.iloc[1]["total_spent"] == 7


def test_sales_by_location():
    stg_sales, stg_product, stg_location, stg_payment = sample_data()
    analytics = SalesAnalytics(stg_sales, stg_product, stg_location, stg_payment)

    df = analytics.sales_by_location()

    assert list(df.columns) == ["location_type", "total_spent"]
    assert df.iloc[0]["location_type"] == "Takeaway"   # 5 + 7 = 12
    assert df.iloc[1]["location_type"] == "In-Store"    # 3


def test_sales_by_payment():
    stg_sales, stg_product, stg_location, stg_payment = sample_data()
    analytics = SalesAnalytics(stg_sales, stg_product, stg_location, stg_payment)

    df = analytics.sales_by_payment()

    assert list(df.columns) == ["payment_method", "total_spent"]
    assert df[df["payment_method"] == "Cash"].iloc[0]["total_spent"] == 8  # 5 + 3
    assert df[df["payment_method"] == "Card"].iloc[0]["total_spent"] == 7


def test_daily_sales():
    stg_sales, stg_product, stg_location, stg_payment = sample_data()
    analytics = SalesAnalytics(stg_sales, stg_product, stg_location, stg_payment)

    df = analytics.daily_sales()

    assert list(df.columns) == ["transaction_date", "total_spent"]
    assert df[df["transaction_date"] == pd.to_datetime("2024-01-01").date()].iloc[0]["total_spent"] == 12
    assert df[df["transaction_date"] == pd.to_datetime("2024-01-02").date()].iloc[0]["total_spent"] == 3
