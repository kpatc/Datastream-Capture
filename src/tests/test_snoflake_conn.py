import snowflake.connector

conn = snowflake.connector.connect(
    account='go08348.af-south-1.aws',
    user='username',
    password='password',
    warehouse='COMPUTE_WH',
    database='CDC_DB',
    schema='PUBLIC'
)
print("✅ Connected to Snowflake!")
