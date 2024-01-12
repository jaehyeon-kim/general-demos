import os
import csv
import dataclasses
import random
import uuid
import json
from typing import List


@dataclasses.dataclass
class User:
    id: int
    first_name: str
    last_name: str
    email: str
    residence: str
    lat: float
    lon: float

    @classmethod
    def load_data(cls, filepath: str):
        items = []
        with open(filepath) as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                items.append(cls(**{**row, **{"id": idx + 1}}))
        return items


@dataclasses.dataclass
class Product:
    id: int
    name: str
    description: str
    price: float
    category: str
    image: str

    @classmethod
    def load_data(cls, filepath: str):
        items = []
        with open(filepath) as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                items.append(cls(**{**row, **{"id": idx + 1}}))
        return items


@dataclasses.dataclass
class OrderItem:
    product_id: int
    quantity: int

    @classmethod
    def create(cls, product: Product):
        return cls(product.id, random.randint(1, 5))


@dataclasses.dataclass
class Order:
    id: int
    user_id: int
    items: str

    @classmethod
    def create(
        self,
        users: List[User],
        products: List[Product],
        num_products: int,
    ):
        user = random.choice(users)
        order_items = [
            OrderItem.create(p) for p in random.choices(products, k=num_products)
        ]
        return Order(
            id=str(uuid.uuid4()),
            user_id=user.id,
            items=json.dumps([dataclasses.asdict(item) for item in order_items]),
        )


if __name__ == "__main__":
    """
    Generate random orders given by the NUM_ORDERS environment variable.
        - Assumed to have users.csv and products.csv files in ./data folder
        - orders.csv will be written to ./data folder
    
    Example:
        python generate_orders.py
        NUM_ORDERS=10000 python generate_orders.py
    """
    NUM_ORDERS = int(os.getenv("NUM_ORDERS", "30000"))
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    users = User.load_data(os.path.join(CURRENT_DIR, "data", "users.csv"))
    products = Product.load_data(
        filepath=os.path.join(CURRENT_DIR, "data", "products.csv")
    )
    orders = [
        Order.create(users, products, random.randint(1, 10)) for _ in range(NUM_ORDERS)
    ]

    filepath = os.path.join(CURRENT_DIR, "data", "orders.csv")
    if os.path.exists(filepath):
        os.remove(filepath)

    with open(os.path.join(CURRENT_DIR, "data", "orders.csv"), "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["id", "user_id", "price", "items", "delivery_lat", "delivery_lon"]
        )
        for order in orders:
            writer.writerow(dataclasses.asdict(order).values())
