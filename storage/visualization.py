import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

def plot_monthly_storage(data):
    fig = px.line(data, x='MONTH', y=['STORAGE', 'STAGE', 'FAILSAFE'],
                  title="Monthly Data Storage over Time")
    fig.update_layout(yaxis_title="Storage (GB)")
    st.plotly_chart(fig)

def plot_daily_storage(data):
    fig = px.line(data, x='USAGE_DATE', y=['STORAGE_GB', 'STAGE_GB', 'FAILSAFE_GB'],
                  title="Daily Data Storage (Last 30 Days)")
    fig.update_layout(yaxis_title="Storage (GB)")
    st.plotly_chart(fig)

def plot_storage_breakdown(data):
    breakdown_pie = px.pie(
        names=["Active", "Stage", "Fail-Safe"],
        values=[
            data["Active Storage (GB)"].iloc[0],
            data["Stage Storage (GB)"].iloc[0],
            data["Failsafe Storage (GB)"].iloc[0]
        ],
        title="Storage Distribution"
    )
    st.plotly_chart(breakdown_pie)

def plot_unused_tables(data):
    top_10_unused = data.nlargest(10, 'ANNUALIZED_STORAGE_COST')
    fig = px.bar(top_10_unused, x='FULLY_QUALIFIED_TABLE_NAME', y='ANNUALIZED_STORAGE_COST',
                 title="Top 10 Unused Tables by Annualized Storage Cost")
    fig.update_layout(xaxis_title="Table Name", yaxis_title="Annualized Storage Cost ($)")
    st.plotly_chart(fig)

def plot_storage_forecast(forecast_data, actual_data):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=forecast_data['USAGE_DATE'], y=forecast_data['FORECAST_GB'], mode='lines', name='Forecast'))
    fig.add_trace(go.Scatter(x=forecast_data['USAGE_DATE'], y=forecast_data['UPPER_BOUND_GB'], mode='lines', name='Upper Bound', line=dict(dash='dash')))
    fig.add_trace(go.Scatter(x=forecast_data['USAGE_DATE'], y=forecast_data['LOWER_BOUND_GB'], mode='lines', name='Lower Bound', line=dict(dash='dash')))
    fig.update_layout(title='Storage Usage Prediction', xaxis_title='Date', yaxis_title='Storage (GB)')
    st.plotly_chart(fig)
