import os
import pandas as pd


def clean_data():
    # Load raw data into DataFrame
    data = pd.read_csv("/tmp/xrate.csv", header=None)

    # Cleanse Data
    default_values = {
        int: 0,
        float: 0.0,
        str: "",
    }

    cleaned_data = data.fillna(value=default_values)

    # Get the current date components
    now = pd.Timestamp.now()
    year = now.year
    month = now.month
    day = now.day

    # Create the directory path if it doesn't exist
    data_dir = f"/opt/airflow/data/xrate_cleansed/{year}/{month}/{day}"
    os.makedirs(data_dir, exist_ok=True)

    # Save the cleaned data to a new file
    cleaned_data.to_csv(f"{data_dir}/xrate.csv", index=False)
