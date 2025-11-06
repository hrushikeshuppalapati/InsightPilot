import streamlit as st
import pandas as pd
from llm_sql_agent import generate_sql, execute_query

st.set_page_config(page_title="InsightPilot", layout="wide")

st.title("🧠 InsightPilot — Conversational Data Analytics")
st.markdown(
    "Ask questions about NYC 311 data in plain English, "
    "and InsightPilot will translate them into SQL queries and show results."
)

with st.sidebar:
    st.header("ℹ️ About")
    st.write(
        "Data source: NYC 311 Open Data API."
    )
    st.divider()
    st.caption("Contributors: Hrushikesh & Chekitha")

user_query = st.text_input("🔍 Enter your question:", placeholder="e.g., Which borough has the most noise complaints?")

if user_query:
    with st.spinner("Generating SQL..."):
        sql_query = generate_sql(user_query)
        st.code(sql_query, language="sql")

        try:
            df = execute_query(sql_query)
            st.success(f"Returned {len(df)} rows")
            st.dataframe(df)

            if len(df.columns) >= 2 and df.dtypes[1] in ["float64", "int64"]:
                st.bar_chart(df.set_index(df.columns[0]))
        except Exception as e:
            st.error(f"Error: {e}")
