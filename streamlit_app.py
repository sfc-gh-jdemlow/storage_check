import streamlit as st
from storage.queries import run_query
from storage.visualization import (
    plot_monthly_storage,
    plot_daily_storage,
    plot_storage_breakdown,
    plot_unused_tables,
    plot_storage_forecast
)
from storage.forecast import generate_storage_forecast
from storage.recommendations import generate_recommendations, display_recommendations


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
plot_monthly_storage(st.session_state.storage_data)

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
plot_daily_storage(st.session_state.daily_storage_data)

# Fetch current storage breakdown if not in session state
if st.session_state.breakdown_data is None:
    breakdown_query = """
    WITH storage_stats AS (
        SELECT 
            STORAGE_BYTES as total_active_bytes,
            STAGE_BYTES as total_stage_bytes,
            FAILSAFE_BYTES as total_failsafe_bytes
        FROM snowflake.account_usage.storage_usage
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
plot_storage_breakdown(st.session_state.breakdown_data)

# Unused Tables Analysis
st.subheader("Unused Tables Analysis")
if 'unused_days' not in st.session_state or 'storage_cost_per_tb' not in st.session_state:
    st.session_state.unused_days = 90
    st.session_state.storage_cost_per_tb = 23.0

col1, col2 = st.columns(2)
with col1:
    unused_days = st.number_input("Days since last access", min_value=1, value=st.session_state.unused_days)
with col2:
    storage_cost_per_tb = st.number_input("Storage cost per TB per month ($)", min_value=0.0, value=st.session_state.storage_cost_per_tb)

if st.session_state.unused_tables is None or unused_days != st.session_state.unused_days or storage_cost_per_tb != st.session_state.storage_cost_per_tb:
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

if st.session_state.unused_tables.empty:
    st.info("No unused tables found based on the specified criteria.")
else:
    st.success(f"Found {len(st.session_state.unused_tables)} unused tables.")
    plot_unused_tables(st.session_state.unused_tables)

# Storage Forecast
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
        st.session_state.forecast_data, st.session_state.actual_data = generate_storage_forecast(training_days, predicted_days)
        st.success("Forecast generated successfully!")
        plot_storage_forecast(st.session_state.forecast_data, st.session_state.actual_data)

        # Storage Cost Estimation
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

# Recommendations
st.subheader("Recommendations")
recommendations = generate_recommendations(
    st.session_state.forecast_data, 
    st.session_state.unused_tables, 
    st.session_state.breakdown_data
)
display_recommendations(recommendations)
