import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import duckdb

try:
    from google.colab import data_table
    data_table.enable_dataframe_formatter()
except ImportError:
    pass

#Loading in data
def paginated_getter():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        paginator=PageNumberPaginator(base_page=1, total_path=None)  
    )
    yield from client.paginate("data_engineering_zoomcamp_api")

#create dlt
ny_taxi = dlt.resource(paginated_getter(), name="ny_taxi_data")

#create DLT Pipeline
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

load_info = pipeline.run(ny_taxi, write_disposition="replace")
print(load_info)

# Connnect with DuckDB
db_path = f"{pipeline.pipeline_name}.duckdb"
print(f"Connecting to database: {db_path}")

conn = duckdb.connect(db_path)

#search path
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

print("Available tables:")
print(conn.sql("SHOW TABLES").df())

table_name = "ny_taxi_data"  
df = pipeline.dataset(dataset_type="default").ny_taxi_data.df()
print(df)

with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM ny_taxi_data;
            """
        )
    # Prints column values of the first row
    print(res)