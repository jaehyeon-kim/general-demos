import os
import time

from google.cloud import pubsub_v1
from google.oauth2 import service_account
from google.api_core import exceptions


class Subscriber:
    def __init__(
        self, project_id: str, topic_name: str, sub_name: str, current_dir: str = None
    ):
        self.subscription = f"projects/{project_id}/subscriptions/{sub_name}"
        self.topic = f"projects/{project_id}/topics/{topic_name}"
        self.current_dir = current_dir or os.path.dirname(os.path.realpath(__file__))
        self.credentials = self.get_credentials()
        self.client = pubsub_v1.SubscriberClient(
            credentials=self.credentials,
        )

    def get_credentials(self):
        sa_key_path = os.path.join(self.current_dir, "sa_key", "key.json")
        return service_account.Credentials.from_service_account_file(
            sa_key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

    def subscription_exists(self):
        try:
            self.client.get_subscription(subscription=self.subscription)
            return True
        except exceptions.NotFound:
            return False
        except Exception as e:
            raise RuntimeError(e) from e

    def create_subscription(self):
        try:
            self.client.create_subscription(name=self.subscription, topic=self.topic)
        except Exception as e:
            raise RuntimeError(e) from e

    def subscribe(self):
        def callback(message):
            print(message)
            message.ack()

        return self.client.subscribe(self.subscription, callback)


if __name__ == "__main__":
    PROJECT_ID = os.environ["PROJECT_ID"]
    TOPIC_NAME = os.getenv("TOPIC_NAME", "input")
    SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME", "sub")

    sub = Subscriber(PROJECT_ID, TOPIC_NAME, SUBSCRIPTION_NAME)
    if not sub.subscription_exists():
        sub.create_subscription()
    future = sub.subscribe()
    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()  # Trigger the shutdown.
        future.result()  # Block until the shutdown is complete.
