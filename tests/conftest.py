import pandas as pd
import pytest

@pytest.fixture
def sample_raw_df():
    return pd.DataFrame({
        "Transaction ID": ["TXN_1001", "TXN_1002"],
        "Item": ["Coffee", "Tea"],
        "Quantity": [1, 2],
        "Price Per Unit": [3.5, 2.0],
        "Total Spent": [3.5, 4.0],
        "Payment Method": ["Card", "Cash"],
        "Location": ["NYC", "LA"],
        "Transaction Date": ["2024-10-01", "2024-10-02"]
    })
