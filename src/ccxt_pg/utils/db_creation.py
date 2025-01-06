import psycopg
from psycopg import sql
from .query_loader import QueryLoader


def create_public_trades_table(credentials: dict, table_name: str):
    """This function checks for the presence of a table named table_name, and creates it if it doesn't exist yet."""
    dbname = credentials["POSTGRES_DB"]
    user = credentials["POSTGRES_USER"]
    password = credentials["POSTGRES_PASSWORD"]
    host = credentials["POSTGRES_HOST"]
    port = credentials["POSTGRES_PORT"]

    with psycopg.connect(
        f"dbname={dbname} user={user} password={password} host={host} port={port}"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(QueryLoader().get_query("create_public_trades_table")).format(
                    sql.Identifier(table_name)
                )
            )
            conn.commit()
