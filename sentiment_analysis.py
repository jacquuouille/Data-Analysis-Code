# Sentiment Analysis - Python Code 
# Summary: 
   # 0. Connecting external data to PostgreSQL database, creating the data table and inserting data into it
   # 1. Creating the 'customer sentiment' table via Python libraries for sentiment analysis 


##
# 0. DATA CONNECTION
##

import psycopg2
import pandas as pd

# Define your PostgreSQL connection details
DB_NAME = "my_db_name"
DB_USER = "my_db_user"
DB_PASSWORD = "my_db_password"
DB_HOST = "my_db_host"
DB_PORT = "my_db_port"

# Read the CSV file (parse dates if necessary)
df = pd.read_csv("/my_pathname/my_file.csv", parse_dates=True)

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
    CREATE TABLE IF NOT EXISTS customer_sentiment (
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
        insert_query = f"INSERT INTO customer_sentiment ({columns}) VALUES ({placeholders})"
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


##
# 1. CUSTOMER SENTIMENT DATASET
##

import pandas as pd
from textblob import TextBlob
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
import re

# Download VADER Lexicon (run once)
nltk.download('vader_lexicon')

# === CONFIGURATION ===
input_csv = "/my_pathname/my_file.csv"  # Input CSV
output_csv = "/my_pathname/my_file.csv"  # Output CSV
text_column = "review_text"  # Original text column

# === LOAD DATA ===
df = pd.read_csv(input_csv)

# Check if column exists
if text_column not in df.columns:
    raise ValueError(f"Column '{text_column}' not found. Available columns: {df.columns.tolist()}")

# === CREATE CLEAN COLUMN ===
clean_column = "review_text_clean"
df[clean_column] = df[text_column]  # Duplicate the original column

# === PREPROCESS CLEAN COLUMN ===
neutral_phrases = [
    "nothing", "nothing,", "nothing.", "nothing!", "nothing, it was really great",
    "niets", "there are no negative things", "nothing to dislike",
    "none", "it was no problem for us", "no complaints"
]

def clean_text(text):
    """
    Remove misleading phrases anywhere in the text.
    Handles punctuation and capitalization.
    """
    if pd.isna(text):
        return ""
    text_clean = str(text)
    for phrase in neutral_phrases:
        pattern = re.compile(re.escape(phrase), re.IGNORECASE)
        text_clean = pattern.sub("", text_clean)
    return text_clean.strip()

# Apply cleaning to the duplicate column
df[clean_column] = df[clean_column].apply(clean_text)

# Initialize VADER
sia = SentimentIntensityAnalyzer()

# === ANALYZE CLEAN COLUMN ===
def analyze_text(text):
    text = str(text)

    # TextBlob sentiment
    blob = TextBlob(text)
    tb_polarity = blob.sentiment.polarity
    tb_subjectivity = blob.sentiment.subjectivity

    # VADER sentiment
    vs = sia.polarity_scores(text)
    vader_compound = vs['compound']
    vader_pos = vs['pos']
    vader_neg = vs['neg']
    vader_neu = vs['neu']

    return pd.Series([tb_polarity, tb_subjectivity,
                      vader_compound, vader_pos, vader_neg, vader_neu])

df[['tb_polarity', 'tb_subjectivity',
    'vader_compound', 'vader_pos', 'vader_neg', 'vader_neu']] = df[clean_column].apply(analyze_text)

# === SAVE OUTPUT ===
df.to_csv(output_csv, index=False)
print(f"Sentiment analysis completed. Results saved to {output_csv}")
