import os
import random
import string
from datetime import date, timedelta
import pandas as pd
from sqlalchemy import create_engine, Connection


def set_conn_str(engine: str, db_name: str):
    if engine == "postgres":
        return f"postgresql+psycopg2://user:password@localhost/{db_name}"
    elif engine == "sqlite":
        return "sqlite://" if db_name is None else f"sqlite:///{db_name}.db"
    else:
        raise RuntimeError(f"Not supported engine - {engine}, only postgres or sqlite")


def generate_df(num_rec: int = 100):
    random.seed(1237)
    d = {
        "val": [random.randint(1, 20) for _ in range(num_rec)],
        "name": [
            "".join(random.choices(string.ascii_lowercase, k=5)) for _ in range(num_rec)
        ],
        "created_at": [
            date(2024, 9, 1) + timedelta(days=random.randint(0, 100))
            for _ in range(num_rec)
        ],
    }
    return pd.DataFrame(d)


def create_postgres(db_name: str = "develop", echo: bool = True):
    return create_engine(set_conn_str("postgres", db_name), echo=echo).connect()


def create_sqlite(db_name: str = "develop", echo: bool = True):
    return create_engine(set_conn_str("sqlite", db_name), echo=echo).connect()


def insert_to_db(
    df: pd.DataFrame,
    tbl_name: str,
    conn: Connection,
    if_exists: str = "replace",
):
    df.to_sql(
        name=tbl_name,
        con=conn,
        index=True,
        if_exists=if_exists,
    )


if __name__ == "__main__":
    engine = os.getenv("ENGINE", "sqlite")
    db_name = "develop"
    conn = create_postgres(db_name) if engine == "postgres" else create_sqlite(db_name)
    df = generate_df(1000)
    insert_to_db(df, tbl_name="demo", conn=conn, if_exists="replace")
