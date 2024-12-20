import os
import random
import string
from datetime import date, timedelta

import daft
from daft.runners import pyrunner
import pandas as pd
import pyarrow as pa
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.table.name_mapping import NameMapping, MappedField

from utils_iceberg import create_catalog

import logging

logging.getLogger().setLevel(logging.DEBUG)

## create catalog and namespace
catalog = create_catalog(
    warehouse_path=os.getcwd(), filename="iceberg_catalog", recreate=True
)
namespace_name = "local"
table_name = "demo"
catalog.create_namespace_if_not_exists(namespace_name)

## create iceberg table
table_identifier = f"{namespace_name}.{table_name}"
d = {
    "id": range(100),
    "val": [random.randint(1, 20) for _ in range(100)],
    "name": ["".join(random.choices(string.ascii_lowercase, k=5)) for _ in range(100)],
    "created_at": [
        date(2024, 9, 1) + timedelta(days=random.randint(0, 100)) for _ in range(100)
    ],
}
df = pd.DataFrame(d)
arrow_df = pa.Table.from_pandas(df)

name_mapping = NameMapping(
    [
        MappedField(field_id=ind + 1, names=[name])
        for ind, name in enumerate(arrow_df.column_names)
    ]
)
iceberg_schema = pyarrow_to_schema(arrow_df.schema, name_mapping)
partition_field = iceberg_schema.find_field("val")
partition_spec = PartitionSpec(
    PartitionField(
        field_id=partition_field.field_id,
        source_id=partition_field.field_id,
        transform=IdentityTransform(),
        name="val",
    )
)

if catalog.table_exists(table_identifier):
    catalog.drop_table(table_identifier)

iceberg_table = catalog.create_table_if_not_exists(
    table_identifier, iceberg_schema, partition_spec=partition_spec
)

iceberg_table.append(arrow_df)


# daft.context.set_runner_py(use_thread_pool=False)

df = daft.read_iceberg(iceberg_table)
df._builder.to_physical_plan_scheduler(daft.context.get_context().daft_execution_config)
df.explain(show_all=True)

df1 = daft.read_iceberg(iceberg_table).where(df["val"] < 3)
df1._builder.to_physical_plan_scheduler(
    daft.context.get_context().daft_execution_config
)
df1._builder.optimize().to_physical_plan_scheduler(
    daft.context.get_context().daft_execution_config
)
df1.explain(show_all=True)

df1.collect()

builder = df1._builder
runner = pyrunner.PyRunner(use_thread_pool=False)
runner.run(builder)
plan_scheduler = builder.to_physical_plan_scheduler(
    daft.context.get_context().daft_execution_config
)
psets = {
    k: v.values() for k, v in runner._part_set_cache.get_all_partition_sets().items()
}
