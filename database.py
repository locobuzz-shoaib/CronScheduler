import time
from typing import Union

import pandas as pd
import pyodbc
from pandas import DataFrame
from config import MSSQL_AUTH


class MssqlHandler:
    def __init__(self, instance_type, force_local=False):
        if instance_type not in ["r", "rw"]:
            raise ValueError("invalid database instance type")
        self.instance_type = "rw"  # instance_type
        self.mssql_conn = None
        self.mssql_cursor = None
        self.retry_sleep = 5
        try:
            print(f"[INFO] connection request from {self.__class__.__name__}")
            print(f"[INFO] connecting, in mode: {self.instance_type}")
            self.reinit_db()
        except (pyodbc.OperationalError, pyodbc.InternalError, pyodbc.IntegrityError, AttributeError):
            text = f"First time Connection to MSSQL Database via MssqlHandler failed, retrying " \
                   f"{2} times"

            for number in range(2):

                try:
                    self.reinit_db()
                    break
                except (pyodbc.OperationalError, pyodbc.InternalError, pyodbc.IntegrityError, AttributeError):
                    self.mssql_conn = None
                    self.mssql_cursor = None
                    time.sleep(self.retry_sleep)
                    pass
            if not self.mssql_conn:
                text = f"In execute(), MSSQL cursor initialisation failed, " \
                       f"after {2} retries"

                raise ConnectionError("Failed to connect to MsSQL db")

    def reinit_db(self):
        intent = "ReadWrite" if self.instance_type == "rw" else "ReadOnly"
        connection_params = {
            "DRIVER": f"{{ODBC Driver 17 for SQL Server}}",
            "UID": MSSQL_AUTH[self.instance_type]["username"],
            "PWD": MSSQL_AUTH[self.instance_type]["password"],
            "ApplicationIntent": intent,
            "SERVER": f'{MSSQL_AUTH[self.instance_type]["IP"]}',
            "DATABASE": MSSQL_AUTH[self.instance_type]["db_name"]
        }

        connection_string = ";".join([f"{key}={value}" for key, value in connection_params.items()])
        print(f'sql db connection string : {connection_string}')
        self.mssql_conn = pyodbc.connect(connection_string)
        self.mssql_cursor = self.mssql_conn.cursor()
        print(f'sql db connected successfully...')

    def reinit_cursor(self):
        try:
            self.mssql_cursor = self.mssql_conn.cursor()
        except (pyodbc.OperationalError, pyodbc.InternalError, pyodbc.IntegrityError, AttributeError):
            self.reinit_db()

    def execute(self, query: str, values: Union[int, str, tuple] = ()):
        self.reinit_cursor()
        query = "\n".join([s.strip() for s in query.split("\n") if s.strip() and s != "\n"])
        try:
            self.mssql_cursor.execute(query, values)
        except (pyodbc.ProgrammingError, pyodbc.DataError) as e:
            error = f"wrong query being executed, {e.__class__.__name__}, {e}, query => {query}"
            raise pyodbc.ProgrammingError
        except (pyodbc.InternalError, pyodbc.IntegrityError, pyodbc.OperationalError, AttributeError) as e:
            error = f"Query Execution failed ,error => {e.__class__.__name__},, query => {query}, {e} retrying connection"
            self.mssql_conn.close()
            self.reinit_db()
            self.retry_connection(query=query)
        except Exception as e:
            error = str(e)
            raise NotImplementedError(error)

    def commit(self):
        self.mssql_conn.commit()

    def retry_connection(self, query: str):
        for number in range(3):
            try:
                self.reinit_cursor()
                if isinstance(self.mssql_cursor, pyodbc.Cursor):
                    self.mssql_cursor.execute(query)
                else:
                    raise pyodbc.OperationalError("Cursor uninitialized")
                break
            except (pyodbc.OperationalError, pyodbc.InternalError, pyodbc.IntegrityError, AttributeError):
                self.mssql_conn = None
                self.mssql_cursor = None
                time.sleep(2)

        if not self.mssql_cursor:
            text = f"In retrying connection, cursor initialisation failed, " \
                   f"query = {query}, after {2} retries"

        return True

    def fetch_tup(self):
        rows = self.mssql_cursor.fetchall()
        return rows

    def fetch_df(self) -> pd.DataFrame:
        headers = [tup[0] for tup in self.mssql_cursor.description]
        df = DataFrame.from_records(self.mssql_cursor.fetchall(), columns=headers)
        return df

    def close(self):
        self.mssql_cursor.close()
        self.mssql_conn.close()