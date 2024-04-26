import asyncio
import time
from typing import Union
import aioodbc
import pandas as pd
import pyodbc
from pandas import DataFrame
from config import MSSQL_AUTH


POOL = None


async def execute(query: str, is_update: bool = False, values: Union[int, str, tuple] = ()):
    count = 3
    global POOL
    query = "\n".join([s.strip() for s in query.split("\n") if s.strip() and s != "\n"])
    df = None
    while count:
        try:
            async with POOL.acquire() as conn:
                async with conn.cursor() as cur:

                    await cur.execute(f"{query}", values)
                    if is_update:
                        await cur.commit()
                    else:
                        headers = [tup[0] for tup in cur.description]
                        df = DataFrame.from_records(await cur.fetchall(), columns=headers)
                    break
        except (pyodbc.ProgrammingError, pyodbc.DataError) as e:
            error = f"wrong query being executed, {e.__class__.__name__}, {e}, query => {query}"
            print(error)
            count -= 1
        except (pyodbc.InternalError, pyodbc.IntegrityError, pyodbc.OperationalError, AttributeError) as e:
            error = f"Query Execution failed ,error => {e.__class__.__name__},, query => {query}, {e} retrying connection"
            print(error)
            count -= 1
        except Exception as e:
            error = str(e)
            # await send_async_message_to_google_chat(f"exception in DB conn: {e}", error_g_chat_hook)
            count -= 1
        time.sleep(5)
    return df


async def create_pool_instance(loop, instance_type: str = "rw"):
    global POOL
    intent = "ReadWrite" if instance_type == "rw" else "ReadOnly"
    connection_params = {
        "DRIVER": f"{{ODBC Driver 17 for SQL Server}}",
        "UID": MSSQL_AUTH[instance_type]["username"],
        "PWD": MSSQL_AUTH[instance_type]["password"],
        "ApplicationIntent": intent,
        "Server": MSSQL_AUTH[instance_type]["IP"],
        "Database": MSSQL_AUTH[instance_type]["db_name"]
    }
    connection_string = ";".join([f"{key}={value}" for key, value in connection_params.items()])
    print(">>> connection string ", connection_string)
    POOL = await aioodbc.create_pool(connection_string, 1, 5, pool_recycle=500, loop=loop)
    print(POOL)
# if __name__ == "__main__":
#     asyncio.run(create_pool_instance())
