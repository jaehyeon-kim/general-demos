import os
import daft
from daft.expressions import col

from utils_iceberg import get_catalog

CATELOG_PATH = f"{os.getcwd()}/simple_catalog.db"
WAREHOUSE_PATH = f"{os.getcwd()}/simple_warehouse"

catalog = get_catalog(
    name="simple", warehouse_path=WAREHOUSE_PATH, catalog_path=CATELOG_PATH
)

iceberg_table = catalog.load_table("demo.sample")

df = daft.read_iceberg(iceberg_table)
df._builder.optimize().to_physical_plan_scheduler(
    daft.context.get_context().daft_execution_config
)
df.collect()

df1 = daft.read_iceberg(iceberg_table).where(col("value") == 2)
df1._builder.optimize().to_physical_plan_scheduler(
    daft.context.get_context().daft_execution_config
)
