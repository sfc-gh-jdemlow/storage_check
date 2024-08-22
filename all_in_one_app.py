import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import warnings
import logging
import os

from datetime import timedelta
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session


def create_snowflake_session(creds: dict = None, **kwargs) -> Session:
    try:
        active_session = get_active_session()
        logging.info("Retrieved active Snowpark session.")
        return active_session
    except Exception as e:
        logging.info(f"No active session found or error retrieving it: {e}")
        if os.path.isfile("/snowflake/session/token"):
            session_config = {
                'host': os.getenv('SNOWFLAKE_HOST'),
                'port': os.getenv('SNOWFLAKE_PORT'),
                'protocol': "https",
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'authenticator': "oauth",
                'token': open('/snowflake/session/token', 'r').read(),
                'warehouse': kwargs.get("warehouse") or os.getenv('SNOWFLAKE_WAREHOUSE'),
                'database': kwargs.get("database") or os.getenv('SNOWFLAKE_DATABASE'),
                'schema': kwargs.get("schema") or os.getenv('SNOWFLAKE_SCHEMA'),
                'client_session_keep_alive': True
            }
        else:
            creds = creds or {}
            session_config = {
                'account': creds.get("account") or os.getenv('SNOWFLAKE_ACCOUNT'),
                'user': creds.get("username") or os.getenv('SNOWFLAKE_USER'),
                'password': creds.get("password") or os.getenv('SNOWFLAKE_PASSWORD'),
                'role': kwargs.get("role") or os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
                'warehouse': kwargs.get("warehouse") or os.getenv('SNOWFLAKE_WAREHOUSE'),
                'database': kwargs.get("database") or os.getenv('SNOWFLAKE_DATABASE'),
                'schema': kwargs.get("schema") or os.getenv('SNOWFLAKE_SCHEMA'),
                'client_session_keep_alive': True
            }
            for key in ['account', 'user', 'password', 'role', 'warehouse', 'database', 'schema']:
                if key not in session_config or not session_config[key]:
                    warnings.warn(f"Missing or empty session configuration for '{key}'.")
            session_config.update(kwargs)

        try:
            session = Session.builder.configs(session_config).create()
            logging.info("Snowpark session successfully created.")
            return session
        except Exception as e:
            logging.info(f"Error creating Snowpark session: {e}")
            return None

def run_query(query):
    df = create_snowflake_session().sql(query).to_pandas()
    return df

def run_command(query):
    df = create_snowflake_session().sql(query).collect()
    return df

# Initialize session state
if 'storage_data' not in st.session_state:
    st.session_state.storage_data = None
if 'daily_storage_data' not in st.session_state:
    st.session_state.daily_storage_data = None
if 'breakdown_data' not in st.session_state:
    st.session_state.breakdown_data = None
if 'forecast_generated' not in st.session_state:
    st.session_state.forecast_generated = False
if 'forecast_data' not in st.session_state:
    st.session_state.forecast_data = None
if 'actual_data' not in st.session_state:
    st.session_state.actual_data = None
if 'unused_tables' not in st.session_state:
    st.session_state.unused_tables = None

# Streamlit app
st.title("Snowflake Storage Analysis")

# Fetch data only if it's not already in the session state
if st.session_state.storage_data is None:
    storage_query = """
    select to_char(usage_date,'YYYYMM') as sort_month,
           to_char(usage_date,'Mon-YYYY') as month,
           avg(storage_bytes) / power(1024, 3) as storage,
           avg(stage_bytes) / power(1024, 3) as stage,
           avg(failsafe_bytes) / power(1024, 3) as failsafe
    from snowflake.account_usage.storage_usage
    group by month, sort_month
    order by sort_month;
    """
    st.session_state.storage_data = run_query(storage_query)

# Visualize monthly storage usage over time
st.subheader("Monthly Storage Usage Over Time")
fig = px.line(st.session_state.storage_data, x='MONTH', y=['STORAGE', 'STAGE', 'FAILSAFE'],
              title="Monthly Data Storage over Time")
fig.update_layout(yaxis_title="Storage (GB)")
st.plotly_chart(fig)

# Fetch daily storage usage data if not in session state
if st.session_state.daily_storage_data is None:
    daily_storage_query = """
    SELECT 
        USAGE_DATE,
        STORAGE_BYTES / POWER(1024, 3) AS STORAGE_GB,
        STAGE_BYTES / POWER(1024, 3) AS STAGE_GB,
        FAILSAFE_BYTES / POWER(1024, 3) AS FAILSAFE_GB
    FROM snowflake.account_usage.storage_usage
    WHERE USAGE_DATE >= DATEADD(day, -30, CURRENT_DATE())
    ORDER BY USAGE_DATE;
    """
    st.session_state.daily_storage_data = run_query(daily_storage_query)

# Visualize daily storage usage
st.subheader("Daily Storage Usage (Last 30 Days)")
fig = px.line(st.session_state.daily_storage_data, x='USAGE_DATE', y=['STORAGE_GB', 'STAGE_GB', 'FAILSAFE_GB'],
              title="Daily Data Storage (Last 30 Days)")
fig.update_layout(yaxis_title="Storage (GB)")
st.plotly_chart(fig)

# Fetch current storage breakdown if not in session state
if st.session_state.breakdown_data is None:
    breakdown_query = """
    WITH storage_stats AS (
        SELECT 
            STORAGE_BYTES as total_active_bytes,
            STAGE_BYTES as total_stage_bytes,
            FAILSAFE_BYTES as total_failsafe_bytes
        FROM (
            SELECT * 
            FROM snowflake.account_usage.storage_usage
            WHERE usage_date < CURRENT_DATE()
        )
        WHERE USAGE_DATE = (SELECT MAX(USAGE_DATE) FROM snowflake.account_usage.storage_usage)
    )
    SELECT 
        ROUND(total_active_bytes / POWER(1024, 3), 1) AS "Active Storage (GB)",
        ROUND(total_stage_bytes / POWER(1024, 3), 1) AS "Stage Storage (GB)",
        ROUND(total_failsafe_bytes / POWER(1024, 3), 1) AS "Failsafe Storage (GB)",
        ROUND((total_stage_bytes / (total_active_bytes + total_stage_bytes + total_failsafe_bytes)) * 100, 1) AS "Stage %",
        ROUND((total_failsafe_bytes / (total_active_bytes + total_stage_bytes + total_failsafe_bytes)) * 100, 1) AS "Fail-Safe %"
    FROM storage_stats;
    """
    st.session_state.breakdown_data = run_query(breakdown_query)

# Display current storage breakdown
st.subheader("Current Storage Breakdown")
st.table(st.session_state.breakdown_data)

# Visualize storage breakdown
breakdown_pie = px.pie(
    names=["Active", "Stage", "Fail-Safe"],
    values=[
        st.session_state.breakdown_data["Active Storage (GB)"].iloc[0],
        st.session_state.breakdown_data["Stage Storage (GB)"].iloc[0],
        st.session_state.breakdown_data["Failsafe Storage (GB)"].iloc[0]
    ],
    title="Storage Distribution"
)
st.plotly_chart(breakdown_pie)

st.subheader("Unused Tables Analysis")

# Initialize or update the session state with the current inputs
if 'unused_days' not in st.session_state or 'storage_cost_per_tb' not in st.session_state:
    st.session_state.unused_days = 90  # Default value
    st.session_state.storage_cost_per_tb = 23.0  # Default value

col1, col2 = st.columns(2)
with col1:
    unused_days = st.number_input("Days since last access", min_value=1, value=st.session_state.unused_days)
with col2:
    storage_cost_per_tb = st.number_input("Storage cost per TB per month ($)", min_value=0.0, value=st.session_state.storage_cost_per_tb)

# Re-run the query only if the inputs have changed
if st.session_state.unused_tables is None or \
   unused_days != st.session_state.unused_days or \
   storage_cost_per_tb != st.session_state.storage_cost_per_tb:
    
    st.session_state.unused_days = unused_days
    st.session_state.storage_cost_per_tb = storage_cost_per_tb

    unused_tables_query = f"""
    WITH
    access_history AS (
        SELECT *
        FROM snowflake.account_usage.access_history
    ),
    access_history_flattened AS (
        SELECT
            access_history.query_id,
            access_history.query_start_time,
            access_history.user_name,
            objects_accessed.value:objectId::integer AS table_id,
            objects_accessed.value:objectName::text AS object_name,
            objects_accessed.value:objectDomain::text AS object_domain,
            objects_accessed.value:columns AS columns_array
        FROM access_history, LATERAL FLATTEN(access_history.base_objects_accessed) AS objects_accessed
    ),
    table_access_history AS (
        SELECT
            query_id,
            query_start_time,
            user_name,
            object_name AS fully_qualified_table_name,
            table_id
        FROM access_history_flattened
        WHERE
            object_domain = 'Table'
            AND table_id IS NOT NULL
    ),
    table_access_summary AS (
        SELECT
            table_id,
            MAX(query_start_time) AS last_accessed_at,
            MAX_BY(user_name, query_start_time) AS last_accessed_by,
            MAX_BY(query_id, query_start_time) AS last_query_id
        FROM table_access_history
        GROUP BY 1
    ),
    table_storage_metrics AS (
        SELECT
            id AS table_id,
            table_catalog || '.' ||table_schema ||'.' || table_name AS fully_qualified_table_name,
            (active_bytes + time_travel_bytes + failsafe_bytes + retained_for_clone_bytes)/POWER(1024,4) AS total_storage_tb,
            total_storage_tb*12*{storage_cost_per_tb} AS annualized_storage_cost
        FROM snowflake.account_usage.table_storage_metrics
        WHERE
            NOT deleted
    )
    SELECT
        table_storage_metrics.*,
        table_access_summary.* EXCLUDE (table_id),
        DATEDIFF(day, last_accessed_at, CURRENT_DATE()) AS days_since_last_access
    FROM table_storage_metrics
    INNER JOIN table_access_summary
        ON table_storage_metrics.table_id=table_access_summary.table_id
    WHERE
        last_accessed_at < DATEADD(day, -{unused_days}, CURRENT_DATE())
    ORDER BY table_storage_metrics.annualized_storage_cost DESC
    """

    with st.spinner("Analyzing unused tables..."):
        st.session_state.unused_tables = run_query(unused_tables_query)

# Display the results
if st.session_state.unused_tables.empty:
    st.info("No unused tables found based on the specified criteria.")
else:
    st.success(f"Found {len(st.session_state.unused_tables)} unused tables.")    
    total_savings = st.session_state.unused_tables['ANNUALIZED_STORAGE_COST'].sum()
    st.write(f"Total potential annual savings: ${total_savings:.2f}")
    st.dataframe(st.session_state.unused_tables)

    top_10_unused = st.session_state.unused_tables.nlargest(10, 'ANNUALIZED_STORAGE_COST')
    fig = px.bar(top_10_unused, x='FULLY_QUALIFIED_TABLE_NAME', y='ANNUALIZED_STORAGE_COST',
                    title="Top 10 Unused Tables by Annualized Storage Cost")
    fig.update_layout(xaxis_title="Table Name", yaxis_title="Annualized Storage Cost ($)")
    st.session_state.unused_fig = fig
    st.plotly_chart(st.session_state.unused_fig)
    
    csv = st.session_state.unused_tables.to_csv(index=False)
    st.download_button(
        label="Download full results as CSV",
        data=csv,
        file_name="unused_tables_analysis.csv",
        mime="text/csv",
    )


# Storage Prediction
st.subheader("Storage Prediction")

if st.button("Generate Storage Forecast"):
    st.session_state.forecast_generated = True

if st.session_state.forecast_generated:
    col1, col2 = st.columns(2)
    with col1:
        training_days = st.number_input("Training Days", min_value=30, value=60)
    with col2:
        predicted_days = st.number_input("Prediction Days", min_value=5, value=30)
    
    if st.button("Run Forecast"):
    
        with st.spinner("Generating forecast..."):
            # Step 1: Create training table
            st.text("Step 1/4: Creating training table...")
            run_command(f"""
            CREATE OR REPLACE TABLE storage_usage_train AS
            SELECT 
                TO_TIMESTAMP_NTZ(usage_date) AS usage_date,
                storage_bytes / POWER(1024, 3) AS storage_gb
            FROM snowflake.account_usage.storage_usage
            WHERE TO_TIMESTAMP_NTZ(usage_date) < DATEADD(day, -{training_days}, CURRENT_DATE());
            """)

            # Step 2: Create forecast model
            st.text("Step 2/4: Creating forecast model...")
            run_command("""
            CREATE OR REPLACE snowflake.ml.forecast storage_forecast_model(
                input_data => system$reference('table', 'storage_usage_train'),
                timestamp_colname => 'usage_date',
                target_colname => 'storage_gb'
            );
            """)

            # Step 3: Generate forecasts
            st.text("Step 3/4: Generating forecasts...")
            run_command(f"""
            CREATE OR REPLACE TABLE storage_forecast_results AS
            SELECT
                ts AS usage_date,
                CASE WHEN forecast < 0 THEN 0 ELSE forecast END AS forecast_gb,
                CASE WHEN lower_bound < 0 THEN 0 ELSE lower_bound END AS lower_bound_gb,
                CASE WHEN upper_bound < 0 THEN 0 ELSE upper_bound END AS upper_bound_gb
            FROM
                TABLE(storage_forecast_model!FORECAST(
                    FORECASTING_PERIODS => {predicted_days},
                    CONFIG_OBJECT => {{'prediction_interval': 0.95}}
                ));
            """)

            # Step 4: Fetch results
            st.text("Step 4/4: Fetching results...")
            forecast_query = """
            SELECT 
                usage_date,
                forecast_gb,
                lower_bound_gb,
                upper_bound_gb
            FROM storage_forecast_results
            ORDER BY usage_date
            """
            st.session_state.forecast_data = run_query(forecast_query)

            actual_data_query = """
            SELECT 
                usage_date,
                storage_bytes / POWER(1024, 3) AS storage_gb
            FROM snowflake.account_usage.storage_usage
            WHERE usage_date >= DATEADD(day, -30, CURRENT_DATE())
            ORDER BY usage_date
            """
            st.session_state.actual_data = run_query(actual_data_query)

        st.success("Forecast generated successfully!")

        # Visualize actual and predicted storage
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=st.session_state.forecast_data['USAGE_DATE'], y=st.session_state.forecast_data['FORECAST_GB'], mode='lines', name='Forecast'))
        fig.add_trace(go.Scatter(x=st.session_state.forecast_data['USAGE_DATE'], y=st.session_state.forecast_data['UPPER_BOUND_GB'], mode='lines', name='Upper Bound', line=dict(dash='dash')))
        fig.add_trace(go.Scatter(x=st.session_state.forecast_data['USAGE_DATE'], y=st.session_state.forecast_data['LOWER_BOUND_GB'], mode='lines', name='Lower Bound', line=dict(dash='dash')))
        fig.update_layout(title='Storage Usage Prediction', xaxis_title='Date', yaxis_title='Storage (GB)')
        st.plotly_chart(fig)

        # Cost Estimation
        st.subheader("Storage Cost Estimation")
        cost_per_tb_per_month = st.number_input("Cost per TB per month ($)", value=23.0)

        last_actual_storage = st.session_state.actual_data['STORAGE_GB'].iloc[-1]
        last_predicted_storage = st.session_state.forecast_data['FORECAST_GB'].iloc[-1]
        last_upper_bound = st.session_state.forecast_data['UPPER_BOUND_GB'].iloc[-1]
        last_lower_bound = st.session_state.forecast_data['LOWER_BOUND_GB'].iloc[-1]

        current_monthly_cost = (last_actual_storage / 1024) * cost_per_tb_per_month
        predicted_monthly_cost = (last_predicted_storage / 1024) * cost_per_tb_per_month
        upper_bound_monthly_cost = (last_upper_bound / 1024) * cost_per_tb_per_month
        lower_bound_monthly_cost = (last_lower_bound / 1024) * cost_per_tb_per_month

        st.write(f"Estimated current monthly storage cost: ${current_monthly_cost:.2f}")
        st.write(f"Estimated monthly storage cost in {predicted_days} days:")
        st.write(f"- Forecast: ${predicted_monthly_cost:.2f}")
        st.write(f"- Upper Bound: ${upper_bound_monthly_cost:.2f}")
        st.write(f"- Lower Bound: ${lower_bound_monthly_cost:.2f}")

        # Clean up created objects
        cleanup_commands = """
        DROP TABLE IF EXISTS storage_usage_train;
        DROP TABLE IF EXISTS storage_usage_predict;
        DROP TABLE IF EXISTS storage_forecast_results;
        DROP MODEL IF EXISTS storage_forecast_model;
        """

        for command in cleanup_commands.split(';'):
            if command.strip():
                run_command(command)

# Provide recommendations
st.subheader("Recommendations")

# Storage breakdown recommendations
if st.session_state.breakdown_data is not None:
    stage_pct = st.session_state.breakdown_data["Stage %"].iloc[0]
    failsafe_pct = st.session_state.breakdown_data["Fail-Safe %"].iloc[0]

recommendations = []

# Storage growth recommendations
if 'forecast_data' in st.session_state and st.session_state.forecast_data is not None and not st.session_state.forecast_data.empty:
    current_storage = st.session_state.forecast_data['FORECAST_GB'].iloc[0]
    future_storage = st.session_state.forecast_data['FORECAST_GB'].iloc[-1]
    growth_rate = (future_storage - current_storage) / current_storage

    if growth_rate > 0.2:
        recommendations.append({
            "type": "warning",
            "title": "High Projected Storage Growth",
            "content": f"""
            - Projected storage growth: {growth_rate:.2%} over the next {len(st.session_state.forecast_data)} days
            - Implement data archiving strategies for old or infrequently accessed data
            - Review and optimize data retention policies
            - Consider compressing large tables or using clustering to improve query performance and reduce storage
            """
        })

# Unused tables recommendations
if 'unused_tables' in st.session_state and st.session_state.unused_tables is not None and not st.session_state.unused_tables.empty:
    total_savings = st.session_state.unused_tables['ANNUALIZED_STORAGE_COST'].sum()
    num_unused_tables = len(st.session_state.unused_tables)
    
    recommendations.append({
        "type": "info",
        "title": "Potential Cost Savings from Unused Tables",
        "content": f"""
        - {num_unused_tables} tables haven't been accessed in the specified period
        - Potential annual savings: ${total_savings:.2f}
        - Review these tables for potential deletion or archiving
        - For critical tables, consider using smaller samples or aggregations instead of full datasets
        """
    })

# General recommendations
recommendations.append({
    "type": "info",
    "title": "General Storage Optimization Tips",
    "content": """
    - Regularly monitor and analyze query patterns to optimize table designs
    - Use appropriate compression techniques for large tables
    - Implement automated processes to clean up temporary and transient objects
    - Periodically review and adjust resource monitors and usage alerts
    - Consider using zero-copy cloning for backup and testing purposes
    """
})

# Display recommendations
for rec in recommendations:
    if rec["type"] == "warning":
        st.warning(rec["title"])
    else:
        st.info(rec["title"])
    st.markdown(rec["content"])

# Code snippet for zero-copy cloning
st.info("Example of zero-copy cloning for backup:")
st.code("CREATE DATABASE backup_db CLONE source_db;")
