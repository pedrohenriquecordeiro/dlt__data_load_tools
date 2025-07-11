import os , logging , traceback
import dlt
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_database
from datetime import datetime
import pytz

# ------------------------------------------------------------------------------
# 1. Logging Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ------------------------------------------------------------------------------
# 2. Load environment variables
# ------------------------------------------------------------------------------

# TO TEST: Uncomment the next two lines to load environment variables from a .env file
# from dotenv import load_dotenv
# load_dotenv()
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sa-data-engineering-processes.json"

MYSQL_DB_USER = "user-reader-app-legacy-cluster"
MYSQL_HOST = "app-legacy-cluster.cluster-ro-wrd12s8fk9.us-east-1.rds.amazonaws.com"
MYSQL_PORT = 3306

# Set destination bucket URL for GCS
os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "gs://onfly-data-lakehouse/databases/app-legacy-cluster/onfly"

# Recommended performance tweak: file max size in bytes (e.g., 50MB)
os.environ["LOADER__FILESYSTEM__CONFIG__LOADER_FILE_MAX_SIZE"] = str(50 * 1024 * 1024) # 50 MB

# ------------------------------------------------------------------------------
# 3. MySQL Connection Setup
# ------------------------------------------------------------------------------
conn_str = (
    f"mysql+pymysql://"
    f"{MYSQL_DB_USER}:"
    f"{os.environ['MYSQL_DB_PASSWORD']}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/onfly"
)
credentials = ConnectionStringCredentials(conn_str)

# ------------------------------------------------------------------------------
# 4. DLT Pipeline Configuration
# ------------------------------------------------------------------------------
pipeline = dlt.pipeline(
    pipeline_name = "mysql_to_gcs",    # pipeline name (used for metadata, logging, etc.)
    destination   = "filesystem",      # destination type
    dataset_name  = "payment_invoice"  # dataset name (used for table names, etc.)
)

# ------------------------------------------------------------------------------
# 5. Source Definition (chunked + incremental)
# ------------------------------------------------------------------------------
source = (
    sql_database(credentials , chunk_size = 10000)
    .with_resources("payment_invoice")
)
source.payment_invoice.apply_hints(
    incremental = dlt.sources.incremental(
        cursor_path   = "updated_at",
        initial_value = None       # You can set an initial timestamp here if needed
    )
)

# ------------------------------------------------------------------------------
# 6. Transformations
# ------------------------------------------------------------------------------


# Add a new column 'inserted_at' with the current timestamp in Sao Paulo timezone
source.payment_invoice.add_map(
    lambda row: {**row, "inserted_at": datetime.now(pytz.timezone('America/Sao_Paulo'))}
)

# Cast columns of the source table
source.payment_invoice.apply_hints(
    columns = {
        "id"                   : {"data_type":"text"},
        "company_id"           : {"data_type":"text"}, 
        "amount"               : {"data_type":"text"}, 
        "status"               : {"data_type":"text"}, 
        "description"          : {"data_type":"text"}, 
        "created_at"           : {"data_type":"timestamp"}, 
        "updated_at"           : {"data_type":"timestamp"}, 
        "payment_id"           : {"data_type":"text"}, 
        "link"                 : {"data_type":"text"}, 
        "due_date"             : {"data_type":"timestamp"}, 
        "type"                 : {"data_type":"text"}, 
        "os"                   : {"data_type":"text"}, 
        "process"              : {"data_type":"text"}, 
        "cost_center_id"       : {"data_type":"text"}, 
        "tag_id"               : {"data_type":"text"}, 
        "deleted_at"           : {"data_type":"timestamp"}, 
        "barcode"              : {"data_type":"text"}, 
        "barcode_typable_line" : {"data_type":"text"}, 
        "nfs_number"           : {"data_type":"text"}, 
        "unified_with"         : {"data_type":"text"}, 
        "email_sent_at"        : {"data_type":"timestamp"},
        "inserted_at"          : {"data_type":"timestamp"}
    }
)

# ------------------------------------------------------------------------------
# 7. Run Pipeline
# ------------------------------------------------------------------------------
try:
    info = pipeline.run(
        source, 
        write_disposition = {"disposition": "merge", "strategy": "upsert"},
        table_format      = "delta",
        primary_key       = ["id"]
    )

    logging.info(info)

except Exception as e:
    logging.error("An unexpected error occurred during pipeline execution.")
    logging.error(str(e))
    traceback.print_exc()
