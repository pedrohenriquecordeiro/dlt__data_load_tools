import os , logging , traceback
import dlt
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_database
from dotenv import load_dotenv

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
load_dotenv()

MYSQL_HOST = "app-legacy-cluster.cluster-ro-chyk4qig2xat.us-east-1.rds.amazonaws.com"
MYSQL_PORT = 3306

os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "gs://corp-data-lakehouse/databases/app-legacy-cluster/corp"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sa-data-engineering-processes.json"

# Recommended performance tweak: file max size in bytes (e.g., 50MB)
os.environ["LOADER__FILESYSTEM__CONFIG__LOADER_FILE_MAX_SIZE"] = str(50 * 1024 * 1024) # 50 MB

# ------------------------------------------------------------------------------
# 3. MySQL Connection Setup
# ------------------------------------------------------------------------------
conn_str = (
    f"mysql+pymysql://"
    f"{os.environ['MYSQL_DB_USER']}:"
    f"{os.environ['MYSQL_DB_PASSWORD']}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/corp"
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
# 6. Run Pipeline with Error Handling and Logging
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
