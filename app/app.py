import sys
import logging
import pymssql
import hvac
import json
import os
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


vault_addr    = os.environ['VAULT_ADDR']
db_server     = os.environ['MSSQL_SERVER']
db            = os.environ['DB']
auth_role     = os.environ['AUTH_ROLE']
db_creds_path = os.environ['DB_CREDS_PATH']

logger.info("INFO: app.py is starting")

logger.info("INFO: vault_addr is    " + vault_addr)
logger.info("INFO: auth_role is     " + auth_role)
logger.info("INFO: db_creds_path is " + db_creds_path)

# Connect to Vault using AWS auth.
try:
    client = hvac.Client(url = vault_addr)
    session = boto3.Session()
    credentials = session.get_credentials()
    client.auth.aws.iam_login(
        role = auth_role, 
        access_key = credentials.access_key, 
        secret_key = credentials.secret_key, 
        session_token = credentials.token
    )
except:
    logger.error("ERROR: Unexpected error: Could not connect to Vault.")
    logger.info("INFO: app.py is exiting")
    raise
    sys.exit()

logger.info("INFO: Successfully authenticated with Vault.")

# Get short-lived MS SQL server credentials from Vault.
try:
    json_data = client.read(db_creds_path)
    username = json_data['data']['username']
    password = json_data['data']['password']
except:
    logger.error("ERROR: Unable to get dynamic MS SQL credentials from Vault.")
    logger.info("INFO: app.py is exiting")
    raise
    sys.exit()

logger.info("INFO: Successfully retrieved DB credentials from Vault.")

logger.info("INFO: db_server is    " + db_server)
logger.info("INFO: db is           " + db)
logger.info("INFO: DB username is: " + username)
logger.info("INFO: DB password is: " + password)

# Connect to MS SQL Server.
try:
    conn = pymssql.connect(
        server   = db_server,
        user     = username,
        password = password,
        database = db
    )
except:
    logger.error("ERROR: Unexpected error: Could not connect to MSSQL server.")
    logger.info("INFO: app.py is exiting")
    raise
    sys.exit()

logger.info("INFO: Successfully connected to MS SQL server")

# Write data to database.
def handler(event, context):
    """
    This function fetches content from MS SQL server
    """
    with conn.cursor() as cur:
        # get number of rows in person table before insert
        sql = 'select count(*) as count from person'
        cur.execute(sql)
        row = cur.fetchone()
        logger.info("INFO: The person table had " + str(row[0]) + " rows")

        # insert data from lambda function
        cur.executemany(
            "INSERT into person (name, email, ssn) VALUES (%s, %s, %s)",
            [(event['name'], event['email'], event['ssn'])]
        )
        conn.commit()

        # get number of rows in person table after insert
        cur.execute(sql)
        row = cur.fetchone()
        logger.info("INFO: The person table now has " + str(row[0]) + " rows")

    return "User %s with password %s added %s rows to the person table" %(username, password, event['name'])

