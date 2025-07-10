import os
import dlt
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_database
from dotenv import load_dotenv

# 1. Load environment variables from .env file
load_dotenv()

MYSQL_HOST = "legacy-cluster.cluster-ro-chyk4qig2xat.us-east-1.rds.amazonaws.com"
MYSQL_PORT = 3306

os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "gs://corp-storage-tables/tables/app-legacy-cluster/corp"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sa-data-engineering-processes.json"

# Create a connection string for MySQL
conn_str = (
    f"mysql+pymysql://"
    f"{os.environ['MYSQL_DB_USER']}:"
    f"{os.environ['MYSQL_DB_PASSWORD']}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/corp"
)
credentials = ConnectionStringCredentials(conn_str)

# 2. Create a DLT pipeline for GCS
pipeline = dlt.pipeline(
    pipeline_name = "mysql_to_gcs",   # pipeline name (used for metadata, logging, etc.)
    destination   = "filesystem",     # destination type,
    dataset_name = "payment_invoice", # dataset name (used for table names, etc.),
    full_refresh  = False,            # set to True to reprocess all data
)

# 3. Define the data source: load only payment_invoice
source = sql_database(credentials).with_resources("payment_invoice")
# Apply incremental hint on the 'updated_at' column so only new/changed rows are fetched
source.payment_invoice.apply_hints(incremental = dlt.sources.incremental("updated_at"))

# 4. Run the pipeline with 'merge' (upsert) write disposition:
info = pipeline.run(
    source, 
    write_disposition = {"disposition": "merge", "strategy": "upsert"},
    table_format      = "delta",
    primary_key       = ["id"]
)
print(info)  # optional: shows number of rows processed, etc.
