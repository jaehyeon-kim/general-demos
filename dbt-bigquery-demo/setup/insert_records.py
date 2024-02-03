import os
import csv
import datetime

from google.cloud import bigquery
from google.oauth2 import service_account


class QueryHelper:
    def __init__(self, current_dir: str = None):
        self.current_dir = current_dir or os.path.dirname(os.path.realpath(__file__))
        self.credentials = self.get_credentials()
        self.client = bigquery.Client(
            credentials=self.credentials, project=self.credentials.project_id
        )

    def get_credentials(self):
        # https://cloud.google.com/bigquery/docs/samples/bigquery-client-json-credentials#bigquery_client_json_credentials-python
        sa_key_path = os.path.join(
            os.path.dirname(self.current_dir), "sa_key", "key.json"
        )
        return service_account.Credentials.from_service_account_file(
            sa_key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

    def get_table(self, dataset_name: str, table_name: str):
        return self.client.get_table(
            f"{self.credentials.project_id}.{dataset_name}.{table_name}"
        )

    def insert_rows(self, dataset_name: str, table_name: str, records: list):
        table = self.get_table(dataset_name, table_name)
        errors = self.client.insert_rows_json(table, records)
        if len(errors) > 0:
            print(errors)
            raise RuntimeError("fails to insert records")


class DataHelper:
    def __init__(self, current_dir: str = None):
        self.current_dir = current_dir or os.path.dirname(os.path.realpath(__file__))

    def load_data(self, file_name: str):
        created_at = datetime.datetime.now().isoformat(timespec="milliseconds")
        records = []
        with open(os.path.join(self.current_dir, "data", file_name), mode="r") as f:
            rows = csv.DictReader(f)
            for ind, row in enumerate(rows):
                extras = {"id": ind + 1, "created_at": created_at}
                records.append({**extras, **row})
        return records


if __name__ == "__main__":
    dataset_name = os.getenv("DATASET_NAME", "pizza_shop")

    query_helper = QueryHelper()
    data_helper = DataHelper()
    print("inserting products...")
    products = data_helper.load_data("products.csv")
    query_helper.insert_rows(dataset_name, "staging_products", products)
    print("inserting users...")
    users = data_helper.load_data("users.csv")
    query_helper.insert_rows(dataset_name, "staging_users", users)
    print("inserting orders...")
    orders = data_helper.load_data("orders.csv")
    query_helper.insert_rows(dataset_name, "staging_orders", orders)
