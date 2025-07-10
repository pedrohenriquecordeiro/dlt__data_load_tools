import os
import dlt
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_database
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST       = "name-database.cluster-ro-chyk4qig2xat.us-east-1.rds.amazonaws.com"
MYSQL_PORT       = 3306

os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = "prj-dev"
os.environ["DESTINATION__BIGQUERY__LOCATION"]     = "us-central1"
os.environ["DESTINATION__BIGQUERY__DATASET_NAME"] = "app_legacy_cluster"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]      = "sa-data-engineering-processes.json"

conn_str = (
    f"mysql+pymysql://"
    f"{os.environ['MYSQL_DB_USER']}:"
    f"{os.environ['MYSQL_DB_PASSWORD']}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/database-name"
)
credentials = ConnectionStringCredentials(conn_str)

# 2. Create a DLT pipeline for BigQuery
pipeline = dlt.pipeline(
    pipeline_name = "mysql_to_bigquery",   # pipeline name (used for metadata, logging, etc.)
    destination   = "bigquery"
)

# 3. Define the data source: load only payment_invoice
source = sql_database(credentials).with_resources("payment_invoice")
#   Apply incremental hint on the 'updated_at' column so only new/changed rows are fetched
# source.payment_invoice.apply_hints(incremental = dlt.sources.incremental("updated_at"))

# 4. Run the pipeline with 'merge' (upsert) write disposition:
info = pipeline.run(
    source, 
    write_disposition = "merge", 
    primary_key = ["id"]
)
print(info)  # optional: shows number of rows processed, etc.
