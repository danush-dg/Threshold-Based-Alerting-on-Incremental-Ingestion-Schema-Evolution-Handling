import json
import boto3
import psycopg2
import logging

# =========================
# AWS CLIENTS
# =========================
secrets_client = boto3.client('secretsmanager')
cloudwatch = boto3.client('cloudwatch')
sns = boto3.client('sns')

SNS_TOPIC = " "
SECRET_NAME = "redshiftcredential"

_cached_creds = None

# =========================
# LOGGING
# =========================
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# =========================
# GET CREDS
# =========================
def get_db_credentials():
    global _cached_creds

    if _cached_creds:
        return _cached_creds

    logger.info("Fetching credentials from Secrets Manager")

    response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(response['SecretString'])

    _cached_creds = {
        "host": secret['host'],
        "user": secret['username'],
        "password": secret['password'],
        "database": secret['database'],
        "port": 5439
    }

    return _cached_creds


# =========================
# CLOUDWATCH METRICS
# =========================
def push_metrics(total_rows):
    cloudwatch.put_metric_data(
        Namespace='ETLMonitoring',
        MetricData=[
            {
                'MetricName': 'RowsProcessed',
                'Value': total_rows,
                'Unit': 'Count'
            }
        ]
    )


# =========================
# METRICS + MONITORING
# =========================
def compute_and_store_metrics(cursor):

    cursor.execute("""
        SELECT row_loaded, stg_tablename, schema_change
        FROM logging_table
        ORDER BY id DESC
        LIMIT 1
    """)
    latest = cursor.fetchone()

    if not latest:
        return 0, 0, 0, 'No'

    current, stg, schema_change = latest

    # rolling avg
    cursor.execute("""
        SELECT AVG(row_loaded)
        FROM logging_table
        WHERE stg_tablename = %s
        AND last_loaded >= GETDATE() - INTERVAL '5 days'
        AND status = 'Success'
    """, (stg,))

    avg = cursor.fetchone()[0] or 0

    deviation = ((current - avg) / avg * 100) if avg > 0 else 0

    # store monitoring
    cursor.execute("""
        INSERT INTO monitoring_table
        (current_rows, avg_rows, deviation_percent, created_at)
        VALUES (%s, %s, %s, GETDATE())
    """, (current, avg, deviation))

    return current, avg, deviation, schema_change


# =========================
# ETL PIPELINE
# =========================
class RedshiftETLPipeline:

    def __init__(self, config):
        self.config = config
        self.conn = None
        self.cursor = None

    def connect(self):
        if not self.conn:
            logger.info("Connecting to Redshift")
            self.conn = psycopg2.connect(**self.config)
            self.cursor = self.conn.cursor()

    def disconnect(self):
        if self.conn:
            self.cursor.close()
            self.conn.close()
            self.conn = None

    def execute_query(self, query, params=None, fetch=False):
        try:
            self.cursor.execute(query, params)

            if fetch:
                cols = [desc[0] for desc in self.cursor.description]
                return [dict(zip(cols, row)) for row in self.cursor.fetchall()]

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Query failed: {e}")
            raise

    # =========================
    # SCHEMA HANDLING
    # =========================
    def get_columns(self, table):
        result = self.execute_query("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table,), fetch=True)

        return {r['column_name']: r['data_type'] for r in result}

    def check_and_update_schema(self, src, stg, op):

        schema_changed = False
        source_cols = self.get_columns(src)
        staging_cols = self.get_columns(stg)

        # NEW
        for col in set(source_cols) - set(staging_cols):
            schema_changed = True
            dtype = source_cols[col]
            self.execute_query(f"ALTER TABLE {stg} ADD COLUMN {col} {dtype}")
            self.execute_query(f"ALTER TABLE {op} ADD COLUMN {col} {dtype}")

        # DROPPED
        for col in set(staging_cols) - set(source_cols):
            schema_changed = True
            self.execute_query(f"ALTER TABLE {stg} DROP COLUMN {col}")
            self.execute_query(f"ALTER TABLE {op} DROP COLUMN {col}")

        # DATATYPE (basic)
        for col in source_cols:
            if col in staging_cols and source_cols[col] != staging_cols[col]:
                schema_changed = True
                dtype = source_cols[col]

                self.execute_query(f"ALTER TABLE {stg} ADD COLUMN {col}_new {dtype}")
                self.execute_query(f"UPDATE {stg} SET {col}_new = {col}")
                self.execute_query(f"ALTER TABLE {stg} DROP COLUMN {col}")
                self.execute_query(f"ALTER TABLE {stg} RENAME COLUMN {col}_new TO {col}")

                self.execute_query(f"ALTER TABLE {op} ADD COLUMN {col}_new {dtype}")
                self.execute_query(f"UPDATE {op} SET {col}_new = {col}")
                self.execute_query(f"ALTER TABLE {op} DROP COLUMN {col}")
                self.execute_query(f"ALTER TABLE {op} RENAME COLUMN {col}_new TO {col}")

        return 'Yes' if schema_changed else 'No'

    # =========================
    # DATA FLOW
    # =========================
    def extract(self, table, last_loaded):
        return self.execute_query(
            f"SELECT * FROM {table} WHERE updated_at >= %s",
            (last_loaded,),
            fetch=True
        )

    def load_staging(self, stg, data):
        if not data:
            return 0

        self.execute_query(f"TRUNCATE {stg}")

        cols = list(data[0].keys())
        col_str = ','.join(cols)
        placeholders = ','.join(['%s'] * len(cols))

        for row in data:
            self.execute_query(
                f"INSERT INTO {stg} ({col_str}) VALUES ({placeholders})",
                tuple(row.values())
            )

        return len(data)

    def upsert(self, op, stg):

        cols = list(self.get_columns(stg).keys())
        col_str = ','.join(cols)

        self.execute_query(f"""
            DELETE FROM {op}
            WHERE id IN (SELECT id FROM {stg})
        """)

        self.execute_query(f"""
            INSERT INTO {op} ({col_str})
            SELECT {col_str} FROM {stg}
        """)

    def update_control(self, src):
        self.execute_query(
            "UPDATE control_table SET last_loaded = GETDATE() WHERE source_table = %s",
            (src,)
        )

    def log(self, stg, count, status, schema_change):
        self.execute_query("""
            INSERT INTO logging_table
            (alert, last_loaded, stg_tablename, row_loaded, status, schema_change)
            VALUES (%s, GETDATE(), %s, %s, %s, %s)
        """, ('No', stg, count, status, schema_change))

    def run(self):
        self.connect()
        total = 0

        for cfg in self.execute_query("SELECT * FROM control_table", fetch=True):

            schema = self.check_and_update_schema(
                cfg['source_table'],
                cfg['staging_table'],
                cfg['operational_table']
            )

            data = self.extract(cfg['source_table'], cfg['last_loaded'])
            count = self.load_staging(cfg['staging_table'], data)

            total += count

            if count:
                self.upsert(cfg['operational_table'], cfg['staging_table'])
                self.update_control(cfg['source_table'])

            self.log(cfg['staging_table'], count, 'Success', schema)

        self.disconnect()
        return total


# =========================
# LAMBDA HANDLER
# =========================
def lambda_handler(event, context):

    logger.info("Lambda started")

    try:
        creds = get_db_credentials()
        etl = RedshiftETLPipeline(creds)

        total = etl.run()

        etl.connect()
        current, avg, deviation, schema_change = compute_and_store_metrics(etl.cursor)

        push_metrics(total)

        alert_flag = 'No'

        if current > avg * 1.10 or schema_change == 'Yes':
            sns.publish(
                TopicArn=SNS_TOPIC,
                Message=f"""
🚨 ETL ALERT

Current Rows   : {current}
5-Day Avg      : {avg}
Deviation (%)  : {deviation:.2f}
Schema Changed : {schema_change}
""",
                Subject="ETL Alert"
            )
            alert_flag = 'Yes'

        summary = f"""
========== ETL SUMMARY ==========
Total Rows Processed : {total}
Current Rows         : {current}
5-Day Avg            : {avg}
Deviation (%)        : {deviation:.2f}
Schema Change        : {schema_change}
Alert Triggered      : {alert_flag}
Status               : SUCCESS
================================
"""

        print(summary)
        logger.info(summary)

        return {"statusCode": 200}

    except Exception as e:
        logger.error(str(e))
        return {"statusCode": 500}
