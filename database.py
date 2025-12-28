import pyodbc
from typing import List, Dict, Any


CONNECTION_STRING = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=HOME-PC;DATABASE=Shop;Trusted_Connection=yes;"


async def get_db_connection():
    try:
        conn = pyodbc.connect(CONNECTION_STRING, autocommit=True)
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Database connection error: {sqlstate}")
        raise

async def fetch_all(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    conn = None
    cursor = None
    try:
        conn = await get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        return results
    except pyodbc.Error as e:
        print(f"Error fetching data: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

async def fetch_one(query: str, params: tuple = ()) -> Dict[str, Any] | None:
    conn = None
    cursor = None
    try:
        conn = await get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        columns = [column[0] for column in cursor.description]
        row = cursor.fetchone()
        if row:
            return dict(zip(columns, row))
        return None
    except pyodbc.Error as e:
        print(f"Error fetching data: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

async def execute_procedure(procedure_name: str, params: tuple = ()) -> None:
    conn = None
    cursor = None
    try:
        conn = await get_db_connection()
        cursor = conn.cursor()
        param_placeholders = ','.join(['?' for _ in params])
        query = f"EXEC {procedure_name} {param_placeholders}"
        cursor.execute(query, params)
    except pyodbc.Error as e:
        print(f"Error executing procedure {procedure_name}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

async def execute_function(function_name: str, params: tuple = ()) -> List[Dict[str, Any]]:
    conn = None
    cursor = None
    try:
        conn = await get_db_connection()
        cursor = conn.cursor()
        param_placeholders = ','.join(['?' for _ in params])
        query = f"SELECT * FROM {function_name}({param_placeholders})"
        cursor.execute(query, params)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        return results
    except pyodbc.Error as e:
        print(f"Error executing function {function_name}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

async def execute_scalar_function(query: str, params: tuple = ()) -> Any:
    conn = None
    cursor = None
    try:
        conn = await get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        row = cursor.fetchone()
        if row:
            return row[0]
        return None
    except pyodbc.Error as e:
        print(f"Error executing scalar function: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
