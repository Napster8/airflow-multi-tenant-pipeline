import sqlite3
import pandas as pd
import hashlib

def create_table(db_path, query, table_name):
    """
    Executes the input query `q` 
    to create a new table in the SQLite database at `db_path`.
    The table name should be defined in the query itself.
    Drops the table if it already exists.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        # Drop table if it exists
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(query)
        conn.commit()
        print("Table created successfully.")
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()


def execute_query(db_path, query):
    """
    Executes a SELECT query and returns all results.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        # store in pandas DataFrame
        results = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        return df

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return None
    finally:
        conn.close()

def insert_values(db_path, query, params=None):
    """
    Executes an INSERT (or any DML) query with optional parameters.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        return cursor.lastrowid
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return None
    finally:
        conn.close()

def hash_value(value, salt="shuru"):
    if value is None:
        return None
    salted_value = f"{salt}{value}".encode('utf-8')
    return hashlib.sha256(salted_value).hexdigest()

def anonymize_data(db_path, table_name, columns_to_hash, salt="shuru", batch_size=1000, pk_column=None):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get column names and primary key
    cursor.execute(f"PRAGMA table_info({table_name})")
    table_info = cursor.fetchall()
    column_names = [col[1] for col in table_info]
    if pk_column is None:
        pk_columns = [col[1] for col in table_info if col[5] == 1]
        pk_column = pk_columns[0] if pk_columns else column_names[0]  # Fallback to the first column

    # Check columns to hash exist
    for col in columns_to_hash:
        if col not in column_names:
            raise ValueError(f"Column '{col}' not found in table '{table_name}'.")

    offset = 0
    total_updated = 0
    while True:
        cursor.execute(
            f"SELECT {pk_column}, {', '.join(columns_to_hash)} FROM {table_name} LIMIT ? OFFSET ?",
            (batch_size, offset)
        )
        rows = cursor.fetchall()
        if not rows:
            break

        for row in rows:
            pk_value = row[0]
            hashed_values = [hash_value(val, salt) for val in row[1:]]
            set_clause = ', '.join([f"{col} = ?" for col in columns_to_hash])
            cursor.execute(
                f"UPDATE {table_name} SET {set_clause} WHERE {pk_column} = ?",
                (*hashed_values, pk_value)
            )
            total_updated += 1

        conn.commit()
        offset += batch_size
        print(f"Processed {offset} rows...")

    conn.close()
    print(f"Anonymization complete. Total rows updated: {total_updated}")

def get_table_names(db_path):
    """
    Returns a list of table names in the SQLite database.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        return [table[0] for table in tables]
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return []
    finally:
        conn.close()

def print_db_info(db_path):
    """
     Print table_info in the following format:
     table_1 : col_1, col_2, col_3...
     table_2 : col_1, col_2, col_3...
     and so on...

     
     """
    tables = get_table_names(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        print(f"{table} : {', '.join(column_names)}")
    
    conn.close()

