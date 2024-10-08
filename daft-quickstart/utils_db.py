import random
import string
from datetime import date, timedelta
import pandas as pd
from sqlalchemy import create_engine


def generate_df(num_rec: int = 100):
    random.seed(1237)
    d = {
        "id": range(num_rec),
        "num": random.randint(1, 100),
        "name": [
            "".join(random.choices(string.ascii_lowercase, k=5)) for _ in range(num_rec)
        ],
        "created_at": [
            date(2024, 9, 1) + timedelta(days=random.randint(0, 100))
            for _ in range(num_rec)
        ],
    }
    return pd.DataFrame(d)


def create_postgres():
    return create_engine(
        "postgresql+psycopg2://devuser:password@localhost/devdb", echo=True
    ).connect()


def create_sqlite(db_name: str = "example", echo: bool = True):
    return create_engine(
        "sqlite://" if db_name is None else f"sqlite:///{db_name}.db", echo=echo
    ).connect()


def insert_to_sqlite(
    df: pd.DataFrame,
    tbl_name: str = "example",
    db_name: str = "example",
    echo: bool = True,
    if_exists: str = "replace",
):
    con_str = "sqlite://" if db_name is None else f"sqlite:///{db_name}.db"
    df.to_sql(
        name=tbl_name,
        con=create_engine(con_str, echo=echo),
        if_exists=if_exists,
    )


if __name__ == "__main__":
    df = generate_df(1000)
    insert_to_sqlite(df)
