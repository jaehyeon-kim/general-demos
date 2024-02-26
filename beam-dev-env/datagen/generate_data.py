import os
import json
import argparse
import datetime
import math
from faker import Faker
import geocoder
import random
from kafka import KafkaProducer


class Producer:
    def __init__(self, bootstrap_servers: list, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = self.create()

    def create(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send(self, event: dict):
        try:
            self.producer.send(self.topic, key={"event_id": event["id"]}, value=event)
        except Exception as e:
            raise RuntimeError("fails to send a message") from e


class EventGenerator:
    def __init__(
        self,
        source: str,
        max_num_users: int,
        max_num_events: int,
        max_lag_secs: int,
        bootstrap_servers: list,
        topic_name: str,
        process_id: int = os.getpid(),
    ):
        self.source = source
        self.max_num_users = max_num_users
        self.max_num_events = max_num_events
        self.max_lag_seconds = max_lag_secs
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.pid = process_id
        self.user_pool = self.create_user_pool()
        self.kafka_producer = self.create_producer()

    def create_user_pool(self):
        init_fields = [
            "ip",
            "id",
            "lat",
            "lng",
            "user_agent",
            "age_bracket",
            "opted_into_marketing",
        ]
        user_pool = []
        for _ in range(self.max_num_users):
            user_pool.append(dict(zip(init_fields, self.set_initial_values())))
        return user_pool

    def create_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def set_initial_values(self, faker=Faker()):
        ip = faker.ipv4()
        lookup = geocoder.ip(ip)
        lat, lng = lookup.latlng if len(lookup.latlng) == 2 else ["", ""]
        id = str(hash(f"{ip}{lat}{lng}"))
        user_agent = random.choice(
            [
                faker.firefox,
                faker.chrome,
                faker.safari,
                faker.internet_explorer,
                faker.opera,
            ]
        )()
        age_bracket = random.choice(["18-25", "26-40", "41-55", "55+"])
        opted_into_marketing = random.choice([True, False])
        return ip, id, lat, lng, user_agent, age_bracket, opted_into_marketing

    def set_req_info(self):
        uri = random.choice(
            [
                "home.html",
                "archea.html",
                "archaea.html",
                "bacteria.html",
                "eucharya.html",
                "protozoa.html",
                "amoebozoa.html",
                "chromista.html",
                "cryptista.html",
                "plantae.html",
                "coniferophyta.html",
                "fungi.html",
                "blastocladiomycota.html",
                "animalia.html",
                "acanthocephala.html",
            ]
        )
        file_size_bytes = random.choice(range(100, 500))
        http_request = f"{random.choice(['GET'])} {uri} HTTP/1.0"
        http_response = random.choice([200])
        return http_request, http_response, file_size_bytes

    def append_to_file(self, event: dict):
        parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        with open(os.path.join(parent_dir, "events", f"{self.pid}.out"), "a") as fp:
            fp.write(f"{json.dumps(event)}\n")

    def send_to_kafka(self, event: dict):
        try:
            self.kafka_producer.send(
                self.topic_name, key={"event_id": event["id"]}, value=event
            )
            self.kafka_producer.flush()
        except Exception as e:
            raise RuntimeError("fails to send a message") from e

    def generate_events(self):
        events = []
        num_events = 0
        while True:
            num_events += 1
            if num_events > self.max_num_events:
                break
            event_ts = datetime.datetime.now() + datetime.timedelta(
                seconds=random.uniform(0, self.max_lag_seconds)
            )
            req_info = dict(
                zip(
                    ["http_request", "http_response", "file_size_bytes"],
                    self.set_req_info(),
                )
            )
            event = {
                **random.choice(self.user_pool),
                **req_info,
                **{
                    "event_datetime": event_ts.isoformat(timespec="seconds"),
                    "event_ts": int(event_ts.timestamp()),
                },
            }
            print(event)
            if self.source == "batch":
                self.append_to_file(event)
            else:
                self.send_to_kafka(event)
            events.append(event)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(__file__, description="Web Server Data Generator")
    parser.add_argument(
        "--source",
        "-s",
        type=str,
        default="batch",
        choices=["batch", "streaming"],
        help="The data source - batch or streaming",
    )
    parser.add_argument(
        "--max_num_users",
        "-u",
        type=int,
        default=1000,
        help="The maximum number of users to create",
    )
    parser.add_argument(
        "--max_num_events",
        "-e",
        type=int,
        default=math.inf,
        help="The maximum number of events to create. Don't set if streaming",
    )
    parser.add_argument(
        "--max_lag_secs",
        "-l",
        type=int,
        default=5,
        help="The maximum seconds that a record can be lagged.",
    )

    args = parser.parse_args()
    source = args.source
    max_num_users = args.max_num_users
    max_num_events = args.max_num_events
    max_lag_secs = args.max_lag_secs
    process_id = os.getpid()

    ## python datagen/generate_data.py -s batch -u 50 -e 10000 -l 6
    ## python datagen/generate_data.py -s streaming -u 50 -e 10000 -l 6
    gen = EventGenerator(
        source,
        max_num_users,
        max_num_events,
        max_lag_secs,
        os.getenv("BOOTSTRAP_SERVERS", "localhost:29092"),
        os.getenv("TOPIC_NAME", "event"),
    )
    gen.generate_events()
