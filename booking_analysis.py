# Python Script: connecting external data to PostgreSQL database, creating the data table and inserting data into it

import psycopg2
import pandas as pd

# Define your PostgreSQL connection details
DB_NAME = "my_db_name"
DB_USER = "my_db_user"
DB_PASSWORD = "my_db_password"
DB_HOST = "my_db_host"
DB_PORT = "my_db_port"

# Read the CSV file (parse dates if necessary)
df = pd.read_csv("/my_pathname/rmy_file.csv", parse_dates=True)

# Function to map pandas dtypes to PostgreSQL types
def map_dtype(column, dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "DECIMAL(10,2)"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    elif df[column].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$').all():
        return "DATE"
    elif df[column].astype(str).str.match(r'^\d{2}:\d{2}(:\d{2})?$').all():
        return "TIME"
    else:
        return "TEXT"  # Default to TEXT for categorical and mixed data

# Initialize conn as None
conn = None

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()
    print("Connected to the database successfully")

    # Dynamically generate the CREATE TABLE statement
    column_definitions = ", ".join([f"{col.replace(' ', '_')} {map_dtype(col, df[col].dtype)}" for col in df.columns])
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS railway_database (
        {column_definitions}
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("Table created successfully (if not exists)")

    # Insert data from CSV dynamically
    columns = ', '.join([col.replace(' ', '_') for col in df.columns])
    placeholders = ', '.join(['%s'] * len(df.columns))

    for _, row in df.iterrows():
        values = [row[col] if pd.notna(row[col]) else None for col in df.columns]
        insert_query = f"INSERT INTO railway_database ({columns}) VALUES ({placeholders})"
        cursor.execute(insert_query, values)

    conn.commit()
    print("Data inserted successfully")

except Exception as e:
    print("Error:", e)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("Database connection closed")
