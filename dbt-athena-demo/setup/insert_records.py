import os
import datetime

import boto3
import botocore
import pandas as pd
import awswrangler as wr


class QueryHelper:
    def __init__(self, db_name: str, bucket_name: str, current_dir: str = None):
        self.db_name = db_name
        self.bucket_name = bucket_name
        self.current_dir = current_dir or os.path.dirname(os.path.realpath(__file__))
        self.glue_client = boto3.client(
            "glue", region_name=os.getenv("AWS_REGION", "ap-southeast-2")
        )

    def check_db(self):
        try:
            self.glue_client.get_database(Name=self.db_name)
            return True
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] == "EntityNotFoundException":
                return False
            else:
                raise err

    def create_db(self):
        try:
            self.glue_client.create_database(
                DatabaseInput={
                    "Name": self.db_name,
                }
            )
            return True
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] == "AlreadyExistsException":
                return True
            else:
                raise err

    def read_source(self, file_name: str):
        df = pd.read_csv(os.path.join(self.current_dir, "data", file_name))
        df.insert(0, "id", range(1, len(df) + 1))
        df.insert(
            df.shape[1],
            "created_at",
            datetime.datetime.now(),
        )
        return df

    def load_source(self, df: pd.DataFrame, obj_name: str):
        if obj_name not in ["users", "products", "orders"]:
            raise ValueError("object name should be one of users, products, orders")
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{self.bucket_name}/staging/{obj_name}/",
            dataset=True,
            database=self.db_name,
            table=f"staging_{obj_name}",
            boto3_session=boto3.Session(
                region_name=os.getenv("AWS_REGION", "ap-southeast-2")
            ),
        )


if __name__ == "__main__":
    query_helper = QueryHelper(db_name="pizza_shop", bucket_name="dbt-pizza-shop-demo")
    if not query_helper.check_db():
        query_helper.create_db()
    print("inserting products...")
    products = query_helper.read_source("products.csv")
    query_helper.load_source(products, "products")
    print("inserting users...")
    users = query_helper.read_source("users.csv")
    query_helper.load_source(users, "users")
    print("inserting orders...")
    orders = query_helper.read_source("orders.csv")
    query_helper.load_source(orders, "orders")
