import os
import shutil
from pyiceberg.catalog.sql import SqlCatalog
from pyspark.sql import SparkSession

# https://kevinjqliu.substack.com/p/a-tour-of-iceberg-catalogs-with-pyiceberg
# https://medium.com/@tglawless/getting-started-with-pyiceberg-a2fc1caffdab
# https://stackoverflow.com/questions/78933802/how-to-create-a-partitioned-table-in-python-using-pyiceberg-with-pyarrow


def get_spark_session(catalog_name, catalog_path: str, warehouse_path: str):
    return (
        SparkSession.builder.appName("IcebergLocalDevelopment")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.xerial:sqlite-jdbc:3.34.0",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config(
            f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(f"spark.sql.catalog.{catalog_name}.type", "jdbc")
        .config(f"spark.sql.catalog.{catalog_name}.uri", f"jdbc:sqlite:{catalog_path}")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
        .getOrCreate()
    )


def clean_up(catalog_path: str, warehouse_path: str):
    if os.path.exists(catalog_path):
        os.remove(catalog_path)
    shutil.rmtree(warehouse_path, ignore_errors=True)


def get_catalog(name: str, warehouse_path: str, catalog_path: str):
    return SqlCatalog(
        name,
        **{
            "uri": f"sqlite:///{catalog_path}",
            "warehouse": f"file://{warehouse_path}",
        },
    )


def create_catalog(
    warehouse_path: str = None,
    filename: str = "pyiceberg_catalog",
    recreate: bool = True,
):
    if warehouse_path is None:
        warehouse_path = os.path.dirname(os.path.realpath(__file__))
    if recreate and os.path.isfile(f"{warehouse_path}/{filename}.db"):
        os.remove(f"{warehouse_path}/{filename}.db")
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/{filename}.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    # catalog.properties
    return catalog


# schema = Schema(
#     NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
#     NestedField(field_id=2, name="name", field_type=StringType(), required=True),
#     NestedField(field_id=3, name="value", field_type=IntegerType(), required=True),
# )

# catalog.create_namespace_if_not_exists("metrics")

# iceberg_table = catalog.create_table_if_not_exists(
#     identifier="metrics.data_points", schema=schema
# )
# # iceberg_table.schema()

# # create the PyArrow table using the Iceberg table's schema.
# pa_table_data = pa.Table.from_pylist(
#     [
#         {"id": 1, "name": "metric_1", "value": 5},
#         {"id": 2, "name": "metric_2", "value": 10},
#         {"id": 3, "name": "metric_1", "value": 5},
#         {"id": 4, "name": "metric_2", "value": 10},
#         {"id": 5, "name": "metric_1", "value": 5},
#     ],
#     schema=iceberg_table.schema().as_arrow(),
# )
# iceberg_table.append(df=pa_table_data)
# # iceberg_table.scan().to_arrow()

# iceberg_table.scan(row_filter=EqualTo("id", 1)).to_arrow()

# ## daft
# df = daft.read_iceberg(iceberg_table)

# df._builder.to_physical_plan_scheduler(daft.context.get_context().daft_execution_config)
