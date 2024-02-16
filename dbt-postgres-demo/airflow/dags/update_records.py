import os
import json
import dataclasses
import random
import string

import psycopg2
import psycopg2.extras


class DbHelper:
    def __init__(self) -> None:
        self.conn = self.connect_db()

    def connect_db(self):
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "postgres"),
            database=os.getenv("DB_NAME", "devdb"),
            user=os.getenv("DB_USER", "devuser"),
            password=os.getenv("DB_PASSWORD", "password"),
        )
        conn.autocommit = False
        return conn

    def get_connection(self):
        if (self.conn is None) or (self.conn.closed):
            self.conn = self.connect_db()

    def fetch_records(self, stmt: str):
        self.get_connection()
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(stmt)
            return cur.fetchall()

    def update_records(self, stmt: str, records: list, to_fetch: bool = True):
        self.get_connection()
        with self.conn.cursor() as cur:
            values = psycopg2.extras.execute_values(cur, stmt, records, fetch=to_fetch)
            self.conn.commit()
            if to_fetch:
                return values

    def commit(self):
        if not self.conn.closed:
            self.conn.commit()

    def close(self):
        if self.conn and (not self.conn.closed):
            self.conn.close()


@dataclasses.dataclass
class Product:
    id: int
    name: int
    description: int
    price: float
    category: str
    image: str

    def __hash__(self) -> int:
        return self.id

    @classmethod
    def from_json(cls, r: dict):
        return cls(**r)

    @staticmethod
    def load(db: DbHelper):
        stmt = """
        WITH windowed AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
            FROM staging.products
        )
        SELECT id, name, description, price, category, image
        FROM windowed
        WHERE rn = 1;
        """
        return [Product.from_json(r) for r in db.fetch_records(stmt)]

    @staticmethod
    def update(db: DbHelper, percent: float = 0.5):
        stmt = "INSERT INTO staging.products(id, name, description, price, category, image) VALUES %s RETURNING id, price"
        products = Product.load(db)
        records = set(random.choices(products, k=int(len(products) * percent)))
        for r in records:
            r.price = r.price + 10
        values = db.update_records(
            stmt, [list(dataclasses.asdict(r).values()) for r in records], True
        )
        return values


@dataclasses.dataclass
class User:
    id: int
    first_name: str
    last_name: str
    email: str
    residence: str
    lat: float
    lon: float

    def __hash__(self) -> int:
        return self.id

    @classmethod
    def from_json(cls, r: dict):
        return cls(**r)

    @staticmethod
    def load(db: DbHelper):
        stmt = """
        WITH windowed AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
            FROM staging.users
        )
        SELECT id, first_name, last_name, email, residence, lat, lon
        FROM windowed
        WHERE rn = 1;
        """
        return [User.from_json(r) for r in db.fetch_records(stmt)]

    @staticmethod
    def update(db: DbHelper, percent: float = 0.5):
        stmt = "INSERT INTO staging.users(id, first_name, last_name, email, residence, lat, lon) VALUES %s RETURNING id, email"
        users = User.load(db)
        records = set(random.choices(users, k=int(len(users) * percent)))
        for r in records:
            r.email = f"{''.join(random.choices(string.ascii_letters, k=5)).lower()}@email.com"
        values = db.update_records(
            stmt, [list(dataclasses.asdict(r).values()) for r in records], True
        )
        return values


@dataclasses.dataclass
class Order:
    user_id: int
    items: str

    @classmethod
    def create(cls):
        order_items = [
            {"product_id": id, "quantity": random.randint(1, 5)}
            for id in set(random.choices(range(1, 82), k=random.randint(1, 10)))
        ]
        return cls(
            user_id=random.randint(1, 10000),
            items=json.dumps([item for item in order_items]),
        )

    @staticmethod
    def append(db: DbHelper, num_orders: int = 5000):
        stmt = "INSERT INTO staging.orders(user_id, items) VALUES %s RETURNING id"
        records = [Order.create() for _ in range(num_orders)]
        values = db.update_records(
            stmt, [list(dataclasses.asdict(r).values()) for r in records], True
        )
        return values


def main():
    db = DbHelper()
    ## update product and user records
    print(f"{len(Product.update(db))} product records updated")
    print(f"{len(User.update(db))} user records updated")
    ## created order records
    print(f"{len(Order.append(db))} order records created")


if __name__ == "__main__":
    os.environ["DB_HOST"] = "localhost"

    db = DbHelper()
    records = [Order.create() for _ in range(100)]
    print(records)
    # ## update product and user records
    # print(f"{len(Product.update(db))} product records updated")
    # print(f"{len(User.update(db))} user records updated")
    # ## created order records
    # print(f"{len(Order.append(db))} order records created")
