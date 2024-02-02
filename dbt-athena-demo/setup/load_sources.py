import os
import datetime

import boto3
import botocore
import pandas as pd
import awswrangler as wr


def check_db(name: str):
    client = boto3.client("glue", region_name=os.getenv("AWS_REGION", "ap-southeast-2"))
    try:
        client.get_database(Name=name)
        return True
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "EntityNotFoundException":
            return False
        else:
            raise err


def create_db(name: str):
    client = boto3.client("glue", region_name=os.getenv("AWS_REGION", "ap-southeast-2"))
    try:
        client.create_database(
            DatabaseInput={
                "Name": name,
            }
        )
        return True
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "AlreadyExistsException":
            return True
        else:
            raise err


def read_source(filepath: str):
    df = pd.read_csv(filepath)
    df.insert(0, "id", range(1, len(df) + 1))
    df.insert(
        df.shape[1],
        "created_at",
        datetime.datetime.now().isoformat(timespec="seconds"),
    )
    return df


def load_source(df: pd.DataFrame, obj_name: str):
    if obj_name not in ["users", "products", "orders"]:
        raise ValueError("object name should be one of users, products, orders")
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{BUCKET_NAME}/staging/{obj_name}/",
        dataset=True,
        database=DATABASE_NAME,
        table=f"staging_{obj_name}",
        boto3_session=boto3.Session(
            region_name=os.getenv("AWS_REGION", "ap-southeast-2")
        ),
    )


if __name__ == "__main__":
    BUCKET_NAME = "dbt-athena-demo-ap-southeast-2"
    DATABASE_NAME = "dbt_athena"
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    print("loading users...")
    users = read_source(os.path.join(CURRENT_DIR, "data", "users.csv"))
    load_source(users, "users")
    print("loading products...")
    products = read_source(os.path.join(CURRENT_DIR, "data", "products.csv"))
    load_source(products, "products")
    print("loading orders...")
    orders = read_source(os.path.join(CURRENT_DIR, "data", "orders.csv"))
    load_source(orders, "orders")
