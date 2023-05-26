import re
import io
from fastavro import writer, parse_schema
import awswrangler as wr
import pandas as pd
import boto3

s3 = boto3.client("s3")

avro_schema = {
    "doc": "User details",
    "name": "User",
    "namespace": "user",
    "type": "record",
    "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}],
}


def check_fields(df: pd.DataFrame, schema: dict):
    if schema.get("fields") is None:
        raise Exception("missing fields in schema keys")
    if len(set(df.columns) - set([f["name"] for f in schema["fields"]])) > 0:
        raise Exception("missing columns in schema key of fields")


def check_data_types(df: pd.DataFrame, schema: dict):
    dtypes = df.dtypes.to_dict()
    for field in schema["fields"]:
        match_type = "object" if field["type"] == "string" else field["type"]
        if re.search(match_type, str(dtypes[field["name"]])) is None:
            raise Exception(f"incorrect column type - {field['name']}")


def generate_avro_file(df: pd.DataFrame, schema: dict):
    check_fields(df, schema)
    check_data_types(df, schema)
    buffer = io.BytesIO()
    writer(buffer, parse_schema(schema), df.to_dict("records"))
    buffer.seek(0)
    return buffer


def lambda_handler(event, context):
    # get bucket and key values
    record = next(iter(event["Records"]))
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]
    file_name = re.sub(".csv$", "", key.split("/")[-1])
    # read input csv as a data frame
    input_path = f"s3://{bucket}/{key}"
    input_df = wr.s3.read_csv([input_path])
    # write to s3 as a parquet file
    wr.s3.to_parquet(df=input_df, path=f"s3://{bucket}/output/{file_name}.parquet")
    # write to s3 as an avro file
    s3.upload_fileobj(generate_avro_file(input_df, avro_schema), bucket, f"output/{file_name}.avro")
