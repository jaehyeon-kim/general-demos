import os
import csv
import dataclasses
import random
import json


@dataclasses.dataclass
class Order:
    user_id: int
    items: str

    @staticmethod
    def create():
        order_items = [
            {"product_id": id, "quantity": random.randint(1, 5)}
            for id in set(random.choices(range(1, 82), k=random.randint(1, 10)))
        ]
        return Order(
            user_id=random.randint(1, 10000),
            items=json.dumps([item for item in order_items]),
        )


if __name__ == "__main__":
    """
    Generate random orders given by the NUM_ORDERS environment variable.
        - orders.csv will be written to ./data folder
    
    Example:
        python generate_orders.py
        NUM_ORDERS=10000 python generate_orders.py
    """
    NUM_ORDERS = int(os.getenv("NUM_ORDERS", "20000"))
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    orders = [Order.create() for _ in range(NUM_ORDERS)]

    filepath = os.path.join(CURRENT_DIR, "data", "orders.csv")
    if os.path.exists(filepath):
        os.remove(filepath)

    with open(os.path.join(CURRENT_DIR, "data", "orders.csv"), "w") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "items"])
        for order in orders:
            writer.writerow(dataclasses.asdict(order).values())
