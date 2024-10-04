import random
import string
from datetime import date, timedelta

import pandas as pd
import sqlalchemy


def generate_df(num_rec: int = 100):
    random.seed(1237)
    d = {
        "id": range(num_rec),
        "name": [
            "".join(random.choices(string.ascii_lowercase, k=5)) for _ in range(num_rec)
        ],
        "created_at": [
            date(2024, 9, 1) + timedelta(days=random.randint(0, 100))
            for _ in range(num_rec)
        ],
    }
    return pd.DataFrame(d)


def create_engine(db_name: str = "example", echo: bool = True):
    con_str = "sqlite://" if db_name is None else f"sqlite:///{db_name}.db"
    return sqlalchemy.create_engine(con_str, echo=echo)


def create_connection(db_name: str = "example", echo: bool = True):
    return sqlalchemy.create_engine(
        "sqlite://" if db_name is None else f"sqlite:///{db_name}.db", echo=echo
    ).connect()


def insert_records(
    df: pd.DataFrame,
    tbl_name: str = "users",
    db_name: str = "example",
    echo: bool = True,
    if_exists: str = "replace",
):
    df.to_sql(
        name=tbl_name,
        con=create_engine(db_name=db_name, echo=echo),
        if_exists=if_exists,
    )


if __name__ == "__main__":
    insert_records(generate_df(1000))
