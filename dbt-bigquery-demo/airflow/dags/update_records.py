import os
import datetime
import dataclasses
import json
import random
import string

from google.cloud import bigquery
from google.oauth2 import service_account


class QueryHelper:
    def __init__(self, sa_keyfile: str):
        self.sa_keyfile = sa_keyfile
        self.credentials = self.get_credentials()
        self.client = bigquery.Client(
            credentials=self.credentials, project=self.credentials.project_id
        )

    def get_credentials(self):
        # https://cloud.google.com/bigquery/docs/samples/bigquery-client-json-credentials#bigquery_client_json_credentials-python
        return service_account.Credentials.from_service_account_file(
            self.sa_keyfile, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

    def get_table(self, dataset_name: str, table_name: str):
        return self.client.get_table(
            f"{self.credentials.project_id}.{dataset_name}.{table_name}"
        )

    def fetch_rows(self, stmt: str):
        query_job = self.client.query(stmt)
        rows = query_job.result()
        return rows

    def insert_rows(self, dataset_name: str, table_name: str, records: list):
        table = self.get_table(dataset_name, table_name)
        errors = self.client.insert_rows_json(table, records)
        if len(errors) > 0:
            print(errors)
            raise RuntimeError("fails to insert records")


@dataclasses.dataclass
class Product:
    id: int
    name: int
    description: int
    price: float
    category: str
    image: str
    created_at: str = datetime.datetime.now().isoformat(timespec="seconds")

    def __hash__(self) -> int:
        return self.id

    def to_json(self):
        return dataclasses.asdict(self)

    @classmethod
    def from_row(cls, row: bigquery.Row):
        return cls(**dict(row.items()))

    @staticmethod
    def fetch(query_helper: QueryHelper):
        stmt = """
        WITH windowed AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
            FROM `pizza_shop.staging_products`
        )
        SELECT id, name, description, price, category, image
        FROM windowed
        WHERE rn = 1;
        """
        return [Product.from_row(row) for row in query_helper.fetch_rows(stmt)]

    @staticmethod
    def insert(query_helper: QueryHelper, percent: float = 0.5):
        products = Product.fetch(query_helper)
        records = set(random.choices(products, k=int(len(products) * percent)))
        for r in records:
            r.price = r.price + 10
        query_helper.insert_rows(
            "pizza_shop",
            "staging_products",
            [r.to_json() for r in records],
        )
        return records


@dataclasses.dataclass
class User:
    id: int
    first_name: str
    last_name: str
    email: str
    residence: str
    lat: float
    lon: float
    created_at: str = datetime.datetime.now().isoformat(timespec="seconds")

    def __hash__(self) -> int:
        return self.id

    def to_json(self):
        return {
            k: v if k not in ["lat", "lon"] else str(v)
            for k, v in dataclasses.asdict(self).items()
        }

    @classmethod
    def from_row(cls, row: bigquery.Row):
        return cls(**dict(row.items()))

    @staticmethod
    def fetch(query_helper: QueryHelper):
        stmt = """
        WITH windowed AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
            FROM `pizza_shop.staging_users`
        )
        SELECT id, first_name, last_name, email, residence, lat, lon
        FROM windowed
        WHERE rn = 1;
        """
        return [User.from_row(row) for row in query_helper.fetch_rows(stmt)]

    @staticmethod
    def insert(query_helper: QueryHelper, percent: float = 0.5):
        users = User.fetch(query_helper)
        records = set(random.choices(users, k=int(len(users) * percent)))
        for r in records:
            r.email = f"{''.join(random.choices(string.ascii_letters, k=5)).lower()}@email.com"
        query_helper.insert_rows(
            "pizza_shop",
            "staging_users",
            [r.to_json() for r in records],
        )
        return records


@dataclasses.dataclass
class Order:
    id: int
    user_id: int
    items: str
    created_at: str = datetime.datetime.now().isoformat(timespec="seconds")

    def to_json(self):
        return dataclasses.asdict(self)

    @classmethod
    def create(cls, id: int):
        order_items = [
            {"product_id": id, "quantity": random.randint(1, 5)}
            for id in set(random.choices(range(1, 82), k=random.randint(1, 10)))
        ]
        return cls(
            id=id,
            user_id=random.randint(1, 10000),
            items=json.dumps([item for item in order_items]),
        )

    @staticmethod
    def insert(query_helper: QueryHelper, max_id: int, num_orders: int = 5000):
        records = []
        for _ in range(num_orders):
            records.append(Order.create(max_id + 1))
            max_id += 1
        query_helper.insert_rows(
            "pizza_shop",
            "staging_orders",
            [r.to_json() for r in records],
        )
        return records

    @staticmethod
    def get_max_id(query_helper: QueryHelper):
        stmt = "SELECT max(id) AS max FROM `pizza_shop.staging_orders`"
        return next(iter(query_helper.fetch_rows(stmt))).max


def main():
    query_helper = QueryHelper(os.environ["SA_KEYFILE"])
    ## update product and user records
    updated_products = Product.insert(query_helper)
    print(f"{len(updated_products)} product records updated")
    updated_users = User.insert(query_helper)
    print(f"{len(updated_users)} user records updated")
    ## create order records
    max_order_id = Order.get_max_id(query_helper)
    new_orders = Order.insert(query_helper, max_order_id)
    print(f"{len(new_orders)} order records created")


if __name__ == "__main__":
    query_helper = QueryHelper(os.environ["SA_KEYFILE"])
    ## update product and user records
    updated_products = Product.insert(query_helper)
    print(f"{len(updated_products)} product records updated")
    updated_users = User.insert(query_helper)
    print(f"{len(updated_users)} user records updated")
    ## create order records
    max_order_id = Order.get_max_id(query_helper)
    new_orders = Order.insert(query_helper, max_order_id)
    print(f"{len(new_orders)} order records created")
