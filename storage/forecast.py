import streamlit as st
from storage.queries import run_command, run_query

def generate_storage_forecast(training_days, predicted_days):
    # Step 1: Create training table
    st.write("Step 1/4: Creating training table...")
    run_command(f"""
    CREATE OR REPLACE TABLE storage_usage_train AS
    SELECT 
        TO_TIMESTAMP_NTZ(usage_date) AS usage_date,
        storage_bytes / POWER(1024, 3) AS storage_gb
    FROM snowflake.account_usage.storage_usage
    WHERE TO_TIMESTAMP_NTZ(usage_date) < DATEADD(day, -{training_days}, CURRENT_DATE());
    """)

    # Step 2: Create forecast model
    st.write("Step 2/4: Creating forecast model...")
    run_command("""
    CREATE OR REPLACE snowflake.ml.forecast storage_forecast_model(
        input_data => system$reference('table', 'storage_usage_train'),
        timestamp_colname => 'usage_date',
        target_colname => 'storage_gb'
    );
    """)

    # Step 3: Generate forecasts
    st.write("Step 3/4: Generating forecasts...")
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
    st.write("Step 4/4: Fetching forecast results...")
    forecast_query = """
    SELECT 
        usage_date,
        forecast_gb,
        lower_bound_gb,
        upper_bound_gb
    FROM storage_forecast_results
    ORDER BY usage_date
    """
    forecast_data = run_query(forecast_query)

    actual_data_query = """
    SELECT 
        usage_date,
        storage_bytes / POWER(1024, 3) AS storage_gb
    FROM snowflake.account_usage.storage_usage
    WHERE usage_date >= DATEADD(day, -30, CURRENT_DATE())
    ORDER BY usage_date
    """
    actual_data = run_query(actual_data_query)

    # Clean up created objects
    st.write("Cleaning up temporary tables and models...")
    cleanup_commands = """
    DROP TABLE IF EXISTS storage_usage_train;
    DROP TABLE IF EXISTS storage_usage_predict;
    DROP TABLE IF EXISTS storage_forecast_results;
    DROP MODEL IF EXISTS storage_forecast_model;
    """

    for command in cleanup_commands.split(';'):
        if command.strip():
            run_command(command)

    return forecast_data, actual_data
