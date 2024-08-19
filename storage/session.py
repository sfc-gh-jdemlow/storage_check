import os
import logging
import warnings

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
