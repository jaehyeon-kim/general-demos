import os
from pyiceberg.catalog.sql import SqlCatalog

# https://kevinjqliu.substack.com/p/a-tour-of-iceberg-catalogs-with-pyiceberg
# https://medium.com/@tglawless/getting-started-with-pyiceberg-a2fc1caffdab
# https://stackoverflow.com/questions/78933802/how-to-create-a-partitioned-table-in-python-using-pyiceberg-with-pyarrow


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
