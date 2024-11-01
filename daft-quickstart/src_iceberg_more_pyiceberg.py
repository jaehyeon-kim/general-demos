import os
import datetime
import pytz
import daft
from daft.expressions import col
from daft.datatype import TimeUnit, DataType

from utils_iceberg import get_catalog

DataType.timestamp()

daft.DataType.timestamp()

TimeUnit.from_str("s")

CATELOG_PATH = f"{os.getcwd()}/more_catalog.db"
WAREHOUSE_PATH = f"{os.getcwd()}/more_warehouse"

catalog = get_catalog(
    name="more", warehouse_path=WAREHOUSE_PATH, catalog_path=CATELOG_PATH
)

iceberg_table = catalog.load_table("demo.sample")

df = daft.read_iceberg(iceberg_table)
df._builder.optimize().to_physical_plan_scheduler(
    daft.context.get_context().daft_execution_config
)
df.collect()

df1 = daft.read_iceberg(iceberg_table).where(col("id") == 3)
df1._builder.optimize().to_physical_plan_scheduler(
    daft.context.get_context().daft_execution_config
)
df1.show()

df2 = daft.read_iceberg(iceberg_table).where(
    (col("id") == 3) & (col("ts") == datetime.datetime(2024, 1, 1, tzinfo=pytz.utc))
)
df2._builder.optimize().to_physical_plan_scheduler(
    daft.context.get_context().daft_execution_config
)
df2.show()
