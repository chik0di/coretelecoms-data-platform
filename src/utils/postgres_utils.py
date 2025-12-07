from airflow.providers.amazon.aws.hooks.ssm import SsmHook
from sqlalchemy import create_engine, text

def get_db_credentials(return_only=None):
    """
    Args:
        return_only: If specified, return only that credential
                    Options: 'host', 'database', 'user', 'password', 'port', 'schema'
    """
    ssm = SsmHook(aws_conn_id="aws_source_bucket_conn")
    
    credentials = {
        'host': ssm.get_parameter_value(parameter="/coretelecomms/database/db_host"),
        'database': ssm.get_parameter_value(parameter="/coretelecomms/database/db_name"),
        'user': ssm.get_parameter_value(parameter="/coretelecomms/database/db_username"),
        'port': ssm.get_parameter_value(parameter="/coretelecomms/database/db_port"),
        'password': ssm.get_parameter_value(parameter="/coretelecomms/database/db_password"),
        'schema': ssm.get_parameter_value(parameter="/coretelecomms/database/table_schema_name")
    }
    
    if return_only:
        return credentials.get(return_only)
    
    return credentials

def get_postgres_engine():
    creds = get_db_credentials()
    
    engine = create_engine(
        f"postgresql+psycopg2://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}",
        connect_args={
            "keepalives"            :1,
            "keepalives_idle"       :30,
            "keepalives_interval"   :10,
            "keepalives_count"      :5
        }
    )
    return engine

def list_webform_tables():
    schema = get_db_credentials(return_only="schema")
    query = text(f"""
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_name LIKE 'web_form_request_%'
        ORDER BY table_name;
    """)
    engine = get_postgres_engine()
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    return [r[0] for r in rows]

def generate_s3_partitioned_key(prefix: str, table) -> str:
    """
    Generate a partitioned S3 key based on the date inside the tablename.
    Expects a tablename formatted like: web_form_request_2025_11_20
    """
    tablename = table             
    date_str = tablename.split("_")[3:]  # 2025-11-20
    year, month, day = date_str[0], date_str[1], date_str[2]
    parquet_name = f"{prefix}-{year}-{month}-{day}.parquet"
    return f"{prefix}/{year}/{month}/{parquet_name}"
