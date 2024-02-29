import os
import datetime
import dataclasses
import json
import random
import string

import boto3
import pandas as pd
import awswrangler as wr


class QueryHelper:
    def __init__(self, db_name: str, bucket_name: str):
        self.db_name = db_name
        self.bucket_name = bucket_name

    def read_sql_query(self, stmt: str):
        return wr.athena.read_sql_query(
            stmt,
            database=self.db_name,
            boto3_session=boto3.Session(
                region_name=os.getenv("AWS_REGION", "ap-southeast-2")
            ),
        )

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


def update_products(
    query_helper: QueryHelper,
    percent: float = 0.5,
    created_at: datetime.datetime = datetime.datetime.now(),
):
    stmt = """
    WITH windowed AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
        FROM pizza_shop.staging_products
    )
    SELECT id, name, description, price, category, image
    FROM windowed
    WHERE rn = 1;
    """
    products = query_helper.read_sql_query(stmt)
    products.insert(products.shape[1], "created_at", created_at)
    records = products.sample(n=int(products.shape[0] * percent))
    records["price"] = records["price"] + 10
    query_helper.load_source(records, "products")
    return records


def update_users(
    query_helper: QueryHelper,
    percent: float = 0.5,
    created_at: datetime.datetime = datetime.datetime.now(),
):
    stmt = """
    WITH windowed AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
        FROM pizza_shop.staging_users
    )
    SELECT id, first_name, last_name, email, residence, lat, lon
    FROM windowed
    WHERE rn = 1;
    """
    users = query_helper.read_sql_query(stmt)
    users.insert(users.shape[1], "created_at", created_at)
    records = users.sample(n=int(users.shape[0] * percent))
    records[
        "email"
    ] = f"{''.join(random.choices(string.ascii_letters, k=5)).lower()}@email.com"
    query_helper.load_source(records, "users")
    return records


@dataclasses.dataclass
class Order:
    id: int
    user_id: int
    items: str
    created_at: datetime.datetime

    def to_json(self):
        return dataclasses.asdict(self)

    @classmethod
    def create(cls, id: int, created_at: datetime.datetime):
        order_items = [
            {"product_id": id, "quantity": random.randint(1, 5)}
            for id in set(random.choices(range(1, 82), k=random.randint(1, 10)))
        ]
        return cls(
            id=id,
            user_id=random.randint(1, 10000),
            items=json.dumps([item for item in order_items]),
            created_at=created_at,
        )

    @staticmethod
    def insert(
        query_helper: QueryHelper,
        max_id: int,
        num_orders: int = 5000,
        created_at: datetime.datetime = datetime.datetime.now(),
    ):
        records = []
        for _ in range(num_orders):
            records.append(Order.create(max_id + 1, created_at).to_json())
            max_id += 1
        query_helper.load_source(pd.DataFrame.from_records(records), "orders")
        return records

    @staticmethod
    def get_max_id(query_helper: QueryHelper):
        stmt = "SELECT max(id) AS mx FROM pizza_shop.staging_orders"
        df = query_helper.read_sql_query(stmt)
        return next(iter(df.mx))


def main():
    query_helper = QueryHelper(db_name="pizza_shop", bucket_name="dbt-pizza-shop-demo")
    created_at = datetime.datetime.now()
    # update product and user records
    updated_products = update_products(query_helper, created_at=created_at)
    print(f"{len(updated_products)} product records updated")
    updated_users = update_users(query_helper, created_at=created_at)
    print(f"{len(updated_users)} user records updated")
    # create order records
    max_order_id = Order.get_max_id(query_helper)
    new_orders = Order.insert(query_helper, max_order_id, created_at=created_at)
    print(f"{len(new_orders)} order records created")


if __name__ == "__main__":
    query_helper = QueryHelper(db_name="pizza_shop", bucket_name="dbt-pizza-shop-demo")
    created_at = datetime.datetime.now()
    # update product and user records
    updated_products = update_products(query_helper, created_at=created_at)
    print(f"{len(updated_products)} product records updated")
    updated_users = update_users(query_helper, created_at=created_at)
    print(f"{len(updated_users)} user records updated")
    # create order records
    max_order_id = Order.get_max_id(query_helper)
    new_orders = Order.insert(query_helper, max_order_id, created_at=created_at)
    print(f"{len(new_orders)} order records created")
