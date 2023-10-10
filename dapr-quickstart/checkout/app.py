import json
import time
import datetime
import logging
import typing
import random
import uuid
import dataclasses

from dapr.clients import DaprClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


@dataclasses.dataclass
class OrderItem:
    product_id: int
    quantity: int


@dataclasses.dataclass
class Order:
    order_id: str
    ordered_at: datetime.datetime
    user_id: str
    order_items: typing.List[OrderItem]

    def asdict(self):
        return dataclasses.asdict(self)

    def tojson(self):
        return json.dumps(self.asdict(), default=Order.serialize)

    @staticmethod
    def serialize(obj: typing.Any):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat(timespec="milliseconds")
        return obj

    @classmethod
    def auto(cls):
        user_id = str(random.randint(1, 100)).zfill(3)
        order_items = [
            OrderItem(random.randint(1, 1000), random.randint(1, 10))
            for _ in range(random.randint(1, 4))
        ]
        return cls(str(uuid.uuid4()), datetime.datetime.utcnow(), user_id, order_items)


# with DaprClient() as client:
#     while True:
#         order = Order.auto()
#         result = client.publish_event(
#             pubsub_name="orders-pubsub",
#             topic_name="orders",
#             data=order.tojson(),
#             # metadata={"key": order.order_id},
#             data_content_type="application/json",
#         )
#         logging.info(f"Publish data: {order.order_id}")
#         time.sleep(1)

with DaprClient() as client:
    for i in range(1, 10):
        order = {"order_id": i}
        # Publish an event/message using Dapr PubSub
        result = client.publish_event(
            pubsub_name="orderpubsub",
            topic_name="orders",
            data=json.dumps(order),
            data_content_type="application/json",
        )
        logging.info("Published data: " + json.dumps(order))
        time.sleep(1)
