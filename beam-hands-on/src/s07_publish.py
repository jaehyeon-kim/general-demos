import os
import time

from google.cloud import pubsub_v1
from google.oauth2 import service_account
from google.api_core import exceptions


class Publisher:
    def __init__(self, project_id: str, topic_name: str, current_dir: str = None):
        self.topic = f"projects/{project_id}/topics/{topic_name}"
        self.current_dir = current_dir or os.path.dirname(os.path.realpath(__file__))
        self.credentials = self.get_credentials()
        self.client = pubsub_v1.PublisherClient(
            credentials=self.credentials,
        )

    def get_credentials(self):
        sa_key_path = os.path.join(self.current_dir, "sa_key", "key.json")
        return service_account.Credentials.from_service_account_file(
            sa_key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

    def topic_exists(self):
        try:
            self.client.get_topic(topic=self.topic)
            return True
        except exceptions.NotFound:
            return False
        except Exception as e:
            raise RuntimeError(e) from e

    def create_topic(self):
        try:
            self.client.create_topic(name=self.topic)
        except Exception as e:
            raise RuntimeError(e) from e

    def publish(self, event_data: bytes):
        try:
            self.client.publish(self.topic, event_data)
        except Exception as e:
            raise RuntimeError(e) from e

    def publish_counts(self):
        with open(os.path.join(self.current_dir, "counts.csv"), "rb") as lines:
            # skip header
            lines.readline()
            for line in lines:
                print(f"publishing {line} to {self.topic}")
                self.publish(line)
                time.sleep(1)


if __name__ == "__main__":
    PROJECT_ID = os.environ["PROJECT_ID"]
    TOPIC_NAME = os.getenv("TOPIC_NAME", "input")

    pub = Publisher(PROJECT_ID, TOPIC_NAME)
    if not pub.topic_exists():
        pub.create_topic()
    pub.publish_counts()
