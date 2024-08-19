from storage.session import create_snowflake_session

def run_query(query):
    session = create_snowflake_session()
    df = session.sql(query).to_pandas() if session else None
    return df

def run_command(query):
    session = create_snowflake_session()
    df = session.sql(query).collect() if session else None
    return df
