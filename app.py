import streamlit as st
import pandas as pd
from main import (
    connect_to_sql_server, create_sqlalchemy_engine, fetch_table,
    standardize_column_names, trim_whitespaces, normalize_case,
    handle_missing_values, convert_column_types, remove_duplicates,
    remove_extra_titles, detect_outliers,
    validate_phone_numbers, validate_email_addresses,
    show_df_info, capitalize_words
)

# ✅ Custom styled table renderer
def style_table(df, height=300):
    styled_df = df.style.set_properties(**{
        'text-align': 'left',
        'white-space': 'nowrap',
        'max-width': '200px'
    })
    st.dataframe(styled_df, height=height)

st.set_page_config(page_title="SQL Data Cleaner", layout="wide")
st.title("🔗 SQL Server Data Cleaning App")

# ----------------------- Sidebar -----------------------
with st.sidebar:
    st.header("Connect to SQL Server")
    server = st.text_input("Server", placeholder="localhost\\SQLEXPRESS")
    database = st.text_input("Database")
    table_name = st.text_input("Table Name")

    if st.button("Connect and Fetch"):
        conn = connect_to_sql_server(server, database)
        if isinstance(conn, str):
            st.error(conn)
        elif not table_name.strip():
            st.error("Please enter a table name.")
        else:
            try:
                df = fetch_table(conn, table_name)
                st.session_state["original_df"] = df.copy()
                st.session_state["df"] = df
                st.success("Data fetched successfully!")
            except Exception as e:
                st.error(f"Error fetching table: {e}")

# ---------------------- Main UI ------------------------
if "df" in st.session_state:
    df = st.session_state["df"]

    st.subheader("📊 Raw Data Preview")
    style_table(df)

    st.markdown("---")

    st.subheader("🧾 Dataset Information")
    with st.expander("🔍 Dataset Metadata (df.info() like)"):
        style_table(show_df_info(df))

    with st.expander("📈 View Unique Values Per Column"):
        col_to_check = st.selectbox("Select Column", df.columns.tolist())
        if col_to_check:
            unique_vals = df[col_to_check].dropna().unique()
            st.write(f"Total unique values: {len(unique_vals)}")
            style_table(pd.DataFrame(unique_vals, columns=[f"Unique values in '{col_to_check}'"]), height=250)

    st.markdown("---")

    with st.expander("🧹 Standardize Column Names"):
        if st.checkbox("Apply Standardization"):
            df = standardize_column_names(df)

    with st.expander("✂️ Trim Whitespaces & Case Formatting"):
        selected_cols = st.multiselect("Columns to Clean", df.columns.tolist())
        if selected_cols:
            if st.checkbox("Trim Whitespaces"):
                df = trim_whitespaces(df, selected_cols)
            if st.checkbox("Lowercase All"):
                df = normalize_case(df, selected_cols, "lower")
            if st.checkbox("Uppercase All"):
                df = normalize_case(df, selected_cols, "upper")
            if st.checkbox("Capitalize Each Word (e.g., John Doe)"):
                df = capitalize_words(df, selected_cols)

    st.markdown("---")

    with st.expander("📉 Handle Missing Values"):
        if st.checkbox("Enable Missing Value Handling"):
            strategy = st.selectbox("Strategy", ["drop", "impute"])

            if strategy == "drop":
                drop_cols = st.multiselect("Select Columns to Check for Missing Values", df.columns.tolist())
                if drop_cols:
                    rows_with_nulls = df[df[drop_cols].isnull().any(axis=1)]

                    st.markdown("### 🔍 Rows with Missing Values")
                    if not rows_with_nulls.empty:
                        style_table(rows_with_nulls)
                        if st.button("🚫 Drop These Rows"):
                            initial_shape = df.shape
                            df = df.dropna(subset=drop_cols)
                            st.success(f"Dropped {initial_shape[0] - df.shape[0]} rows with missing data in: {', '.join(drop_cols)}")
                    else:
                        st.success("✅ No missing values found in the selected columns.")

            elif strategy == "impute":
                method = st.selectbox("Method", ["mean", "median", "mode"])
                if st.button("🧠 Impute Missing Values"):
                    df = handle_missing_values(df, strategy="impute", method=method)
                    st.success(f"Missing values imputed using {method}.")

    st.markdown("---")

    with st.expander("🔁 Convert Column Data Types"):
        conversion_cols = st.multiselect("Select Columns to Convert", df.columns.tolist())
        conversion_types = {}
        if conversion_cols:
            for col in conversion_cols:
                new_type = st.selectbox(f"Target Type for '{col}'", ["int", "float", "str", "datetime"], key=col)
                conversion_types[col] = new_type
            if st.button("Apply Type Conversion"):
                if conversion_types:
                    df = convert_column_types(df, conversion_types)
                    st.success("Conversion applied.")
                else:
                    st.warning("No columns selected for conversion.")

    st.markdown("---")

    with st.expander("🧬 Remove Duplicates"):
        dup_cols = st.multiselect("Remove Duplicates Based On", df.columns.tolist())
        if dup_cols:
            df = remove_duplicates(df, dup_cols)

    with st.expander("👤 Remove Titles from Names"):
        name_col = st.selectbox("Select Name Column", ["None"] + df.columns.tolist())
        if name_col != "None":
            df = remove_extra_titles(df, name_col)

    st.markdown("---")

    with st.expander("📱 Phone & Email Validation"):
        phone_col = st.selectbox("Phone Number Column", ["None"] + df.columns.tolist())
        if phone_col != "None":
            df = validate_phone_numbers(df, phone_col)
            style_table(df[[phone_col, "valid_phone"]], height=250)

        email_col = st.selectbox("Email Address Column", ["None"] + df.columns.tolist())
        if email_col != "None":
            df = validate_email_addresses(df, email_col)
            style_table(df[[email_col, "valid_email"]], height=250)

    st.markdown("---")

    with st.expander("🚨 Detect Outliers"):
        if st.checkbox("Run Outlier Detection"):
            report = detect_outliers(df)
            st.json(report)

    st.markdown("---")

    st.subheader("🧼 Cleaned Data Preview")
    style_table(df)

    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download Cleaned CSV", csv, file_name="cleaned_sql_data.csv", mime='text/csv')

    backup = st.session_state["original_df"].to_csv(index=False).encode("utf-8")
    st.download_button("📦 Download Backup of Original SQL Data", backup, file_name="original_sql_data.csv", mime="text/csv")

    confirm = st.checkbox("✅ I confirm I want to overwrite the SQL table")

    if st.button("📝 Update SQL Table with Cleaned Data"):
        if not confirm:
            st.warning("Please confirm overwrite before proceeding.")
        else:
            engine = create_sqlalchemy_engine(server, database)
            if isinstance(engine, str):
                st.error(engine)
            else:
                try:
                    df.to_sql(table_name, con=engine, if_exists="replace", index=False, method="multi")
                    st.success("SQL table updated successfully!")
                except Exception as e:
                    st.error(f"Failed to update SQL table: {e}")
