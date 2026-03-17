import json
import boto3
import psycopg2

secrets_client = boto3.client('secretsmanager')
cloudwatch = boto3.client('cloudwatch')
sns = boto3.client('sns')

SNS_TOPIC = "arn:aws:sns:ap-south-1:104487794852:ingestion-alerts"


def get_db_credentials():
    print("Fetching DB credentials from Secrets Manager")

    secret = secrets_client.get_secret_value(
        SecretId="redshift-key"
    )

    creds = json.loads(secret['SecretString'])

    print("Credentials retrieved successfully")

    return creds


def lambda_handler(event, context):

    print("=== PIPELINE STARTED ===")

    creds = get_db_credentials()

    print("Connecting to Redshift...")

    conn = psycopg2.connect(
        host=creds['host'],
        user=creds['username'],
        password=creds['password'],
        database=creds['database'],
        port=creds['port']
    )

    print("Connection established")

    cursor = conn.cursor()

    # STEP 1 Count today's records
    print("\nSTEP 1: Counting today's records")

    cursor.execute("""
        SELECT COUNT(*)
        FROM source_sales
        WHERE sale_date = CURRENT_DATE
    """)

    current_count = cursor.fetchone()[0]

    print(f"Rows ingested today: {current_count}")

    # STEP 2 Calculate rolling average
    print("\nSTEP 2: Calculating 5-day rolling average")

    cursor.execute("""
        SELECT COALESCE(AVG(rows_loaded),0)
        FROM control_table
        WHERE run_date >= CURRENT_DATE - INTERVAL '5 day'
    """)

    avg_last5 = cursor.fetchone()[0]

    if avg_last5 == 0:
        avg_last5 = current_count if current_count != 0 else 1

    print(f"Rolling average (last 5 days): {avg_last5}")

    # STEP 3 Calculate deviation
    deviation = ((current_count - avg_last5) / avg_last5) * 100

    print(f"Deviation from rolling average: {deviation:.2f}%")

    # STEP 4 Update control table
    print("\nSTEP 3: Updating control table")

    cursor.execute("""
        INSERT INTO control_table(run_date, rows_loaded, status)
        VALUES (CURRENT_DATE, %s, 'SUCCESS')
    """, (current_count,))

    print("Control table updated")

    # STEP 5 Insert monitoring metrics
    print("\nSTEP 4: Inserting monitoring metrics")

    cursor.execute("""
        INSERT INTO monitoring_table
        VALUES (CURRENT_DATE,%s,%s,%s)
    """,(current_count,avg_last5,deviation))

    print("Monitoring table updated")

    # STEP 6 Threshold check
    print("\nSTEP 5: Checking threshold condition")

    threshold_triggered = False

    if current_count > avg_last5 * 1.10:

        threshold_triggered = True

        print("ALERT: Threshold exceeded")

        cursor.execute("""
            INSERT INTO alerts_table(run_date,message)
            VALUES (CURRENT_DATE,'Row spike detected')
        """)

        cloudwatch.put_metric_data(
            Namespace='IngestionMonitoring',
            MetricData=[
                {
                    'MetricName':'IngestionSpike',
                    'Value':1,
                    'Unit':'Count'
                }
            ]
        )

        sns.publish(
            TopicArn=SNS_TOPIC,
            Message="Data spike detected in ingestion pipeline"
        )

        print("Alert stored, CloudWatch metric sent, SNS notification triggered")

    else:
        print("No threshold breach detected")

    # STEP 7 Schema evolution detection
    print("\nSTEP 6: Checking schema evolution")

    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='source_sales'
        ORDER BY ordinal_position
    """)

    schema = ",".join([r[0] for r in cursor.fetchall()])

    print(f"Current schema: {schema}")

    cursor.execute("""
        SELECT schema_definition
        FROM schema_history
        ORDER BY version_id DESC
        LIMIT 1
    """)

    last_schema = cursor.fetchone()

    schema_changed = False

    if not last_schema or last_schema[0] != schema:

        schema_changed = True

        cursor.execute("""
            INSERT INTO schema_history (table_name, schema_definition, change_detected_at)
            VALUES (%s,%s,CURRENT_TIMESTAMP)
        """, ("source_sales", schema))

        print("SCHEMA CHANGE DETECTED - new version logged")

    else:
        print("No schema changes detected")

    conn.commit()

    cursor.close()
    conn.close()

    print("\n=== PIPELINE COMPLETED ===")

    print(f"""
FINAL RESULT SUMMARY
---------------------
Rows Ingested Today : {current_count}
Rolling Avg (5 days): {avg_last5}
Deviation (%)       : {deviation:.2f}
Threshold Triggered : {threshold_triggered}
Schema Changed      : {schema_changed}
""")

    return {
        "statusCode":200,
        "body":"Pipeline executed successfully"
    }