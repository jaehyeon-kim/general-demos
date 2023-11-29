import os
import uuid
import random

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment
from pyflink.table.udf import udf

def _qry_source_table():
    stmt = """
    CREATE TABLE seeds (
        ts  AS PROCTIME()
    ) WITH (
        'connector' = 'datagen',
        'rows-per-second' = '3'
    )
    """
    print(stmt)
    return stmt

def _qry_sink_table(table_name: str, topic_name: str, bootstrap_servers: str):
    stmt = f"""
    CREATE TABLE {table_name} (
        `id`        VARCHAR,
        `value`     INT,
        `ts`        TIMESTAMP(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic_name}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',        
        'format' = 'json',
        'key.format' = 'json',
        'key.fields' = 'id',
        'properties.allow.auto.create.topics' = 'true'
    )
    """
    print(stmt)
    return stmt


def _qry_print_table():
    stmt = """
    CREATE TABLE print (
        `id`        VARCHAR,
        `value`     INT,
        `ts`        TIMESTAMP(3)  

    ) WITH (
        'connector' = 'print'
    )
    """
    print(stmt)
    return stmt

def _qry_insert(target_table: str):
    stmt = f"""
    INSERT INTO {target_table}
    SELECT
        add_id(),
        add_value(),
        ts    
    FROM seeds
    """
    print(stmt)
    return stmt

if __name__ == "__main__":
    """
    docker exec jobmanager /usr/lib/flink/bin/flink run \
        --python /home/flink/project/apps/flink/producer.py \
        -d
    """

    RUNTIME_ENV = os.environ.get("RUNTIME_ENV", "LOCAL")  # DOCKER or LOCAL
    BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:29092")  # overwrite app config

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    if RUNTIME_ENV == "LOCAL":
        SRC_DIR = os.path.dirname(os.path.realpath(__file__))
        JAR_FILES = ["flink-sql-connector-kafka-1.17.1.jar"] # should exist where producer.py exists
        JAR_PATHS = tuple(
            [f"file://{os.path.join(SRC_DIR, name)}" for name in JAR_FILES]
        )        
        env.add_jars(*JAR_PATHS)
        print(JAR_PATHS)

    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().set_local_timezone("Australia/Sydney")
    t_env.create_temporary_function(
        "add_id", udf(lambda: str(uuid.uuid4()), result_type="STRING")
    )
    t_env.create_temporary_function(
        "add_value", udf(lambda: random.randrange(0, 1000), result_type="INT")
    )
    ## create source and sink tables
    SINK_TABLE_NAME = "orders_table"
    t_env.execute_sql(_qry_source_table())
    t_env.execute_sql(_qry_sink_table(SINK_TABLE_NAME, "orders", BOOTSTRAP_SERVERS))
    t_env.execute_sql(_qry_print_table())
    ## insert into sink table
    if RUNTIME_ENV == "LOCAL":
        statement_set = t_env.create_statement_set()
        statement_set.add_insert_sql(_qry_insert(SINK_TABLE_NAME))
        statement_set.add_insert_sql(_qry_insert("print"))
        statement_set.execute().wait()
    else:
        table_result = t_env.execute_sql(_qry_insert(SINK_TABLE_NAME))
        print(table_result.get_job_client().get_job_status())
