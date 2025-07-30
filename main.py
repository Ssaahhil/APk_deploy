import pandas as pd
import numpy as np
import pyodbc
import re
import time
from sqlalchemy import create_engine
# ------------------ SQL Server Connection ------------------ #

def connect_to_sql_server(server, database):
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};"
        f"Trusted_Connection=yes;"
    )
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        return f"Connection failed: {e}"

def create_sqlalchemy_engine(server, database):
    conn_str = f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    try:
        engine = create_engine(conn_str, fast_executemany=True)
        return engine
    except Exception as e:
        return f"SQLAlchemy engine creation failed: {e}"

def fetch_table(conn, table_name):
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, conn)

# ------------------ Cleaning Functions ------------------ #

def standardize_column_names(df):
    df.columns = [re.sub(r"[^a-zA-Z0-9_]", "_", col.strip().lower()) for col in df.columns]
    return df

def trim_whitespaces(df, cols):
    for col in cols:
        df[col] = df[col].astype(str).str.strip()
    return df

def normalize_case(df, cols, case_type="lower"):
    for col in cols:
        if case_type == "lower":
            df[col] = df[col].astype(str).str.lower()
        else:
            df[col] = df[col].astype(str).str.upper()
    return df

def capitalize_words(df, cols):
    for col in cols:
        df[col] = df[col].astype(str).str.title()
    return df

def handle_missing_values(df, strategy="none", method=None):
    if strategy == "drop":
        df = df.dropna()
    elif strategy == "impute":
        for col in df.select_dtypes(include=np.number).columns:
            if method == "mean":
                df[col] = df[col].fillna(df[col].mean())
            elif method == "median":
                df[col] = df[col].fillna(df[col].median())
            elif method == "mode":
                df[col] = df[col].fillna(df[col].mode()[0])
    return df

def convert_column_types(df, conversions):
    for col, dtype in conversions.items():
        try:
            if dtype == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype == "str":
                df[col] = df[col].astype(str)
            elif dtype == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
        except Exception as e:
            print(f"Error converting {col} to {dtype}: {e}")
    return df


def remove_duplicates(df, subset_cols):
    return df.drop_duplicates(subset=subset_cols)

def remove_extra_titles(df, name_col):
    df[name_col] = df[name_col].astype(str).str.replace(r"(?i)\b(mr|mrs|dr|ms|shri)\.?\s*", "", regex=True)
    return df

def detect_outliers(df):
    outlier_report = {}
    for col in df.select_dtypes(include=np.number).columns:
        z_score = (df[col] - df[col].mean()) / df[col].std()
        count = df[(z_score.abs() > 3)].shape[0]
        if count > 0:
            outlier_report[col] = count
    return outlier_report

# ------------------ Validations ------------------ #

def validate_phone_numbers(df, phone_col):
    pattern = r'^(\+91[-\s]?)?[6-9]\d{9}$'
    df["valid_phone"] = df[phone_col].astype(str).str.fullmatch(pattern).fillna(False)
    return df

def validate_email_addresses(df, email_col):
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    df["valid_email"] = df[email_col].astype(str).str.fullmatch(pattern).fillna(False)
    return df

# ------------------ Utility ------------------ #

def show_df_info(df):
    info_df = pd.DataFrame({
        "Column": df.columns,
        "Data Type": df.dtypes.astype(str),
        "Null Count": df.isnull().sum().values,
        "Unique Values": df.nunique().values
    })
    return info_df
