import pytest
import pandas as pd
from transform import app


@pytest.fixture
def input_df():
    return pd.DataFrame.from_dict({"name": ["Vrinda", "Tracy"], "age": [22, 28]})


def test_generate_avro_file_success(input_df):
    avro_schema = {
        "doc": "User details",
        "name": "User",
        "namespace": "user",
        "type": "record",
        "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}],
    }
    app.generate_avro_file(input_df, avro_schema)
    assert True


def test_generate_avro_file_fail_missing_fields(input_df):
    avro_schema = {
        "doc": "User details",
        "name": "User",
        "namespace": "user",
        "type": "record",
    }
    with pytest.raises(Exception) as e:
        app.generate_avro_file(input_df, avro_schema)
    assert "missing fields in schema keys" == str(e.value)


def test_generate_avro_file_fail_missing_columns(input_df):
    avro_schema = {
        "doc": "User details",
        "name": "User",
        "namespace": "user",
        "type": "record",
        "fields": [{"name": "name", "type": "string"}],
    }
    with pytest.raises(Exception) as e:
        app.generate_avro_file(input_df, avro_schema)
    assert "missing columns in schema key of fields" == str(e.value)


def test_generate_avro_file_fail_incorrect_age_type(input_df):
    avro_schema = {
        "doc": "User details",
        "name": "User",
        "namespace": "user",
        "type": "record",
        "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "string"}],
    }
    with pytest.raises(Exception) as e:
        app.generate_avro_file(input_df, avro_schema)
    assert f"incorrect column type - age" == str(e.value)
