import streamlit as st

def generate_recommendations(forecast_data, unused_tables, breakdown_data):
    recommendations = []

    # Storage growth recommendations
    if forecast_data is not None and not forecast_data.empty:
        current_storage = forecast_data['FORECAST_GB'].iloc[0]
        future_storage = forecast_data['FORECAST_GB'].iloc[-1]
        growth_rate = (future_storage - current_storage) / current_storage

        if growth_rate > 0.2:
            recommendations.append({
                "type": "warning",
                "title": "High Projected Storage Growth",
                "content": f"""
                - Projected storage growth: {growth_rate:.2%} over the next {len(forecast_data)} days
                - Implement data archiving strategies for old or infrequently accessed data
                - Review and optimize data retention policies
                - Consider compressing large tables or using clustering to improve query performance and reduce storage
                """
            })

    # Unused tables recommendations
    if unused_tables is not None and not unused_tables.empty:
        total_savings = unused_tables['ANNUALIZED_STORAGE_COST'].sum()
        num_unused_tables = len(unused_tables)
        
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

    return recommendations

def display_recommendations(recommendations):
    for rec in recommendations:
        if rec["type"] == "warning":
            st.warning(rec["title"])
        else:
            st.info(rec["title"])
        st.markdown(rec["content"])
