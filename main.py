from google.cloud import storage
from google.cloud import bigquery
from google.cloud import bigquery_storage
import pandas as pd
import pytz
import pyarrow
from datetime import datetime

# authentication check
def test_login_BigQuery():
    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    storage_client = storage.Client()

    # Make an authenticated API request
    buckets = list(storage_client.list_buckets())
    print(buckets)


def test_load_BigQuery_csv ():
    # Construct a BigQuery client object.
    client = bigquery.Client()
    # TODO(developer): Set table_id to the ID of the table to create.
    # Note - need to exchange : for . in readme
    table_id = "bigquery-299706.201225BigQuery.Test123"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True, schema=[bigquery.SchemaField("Name", "STRING")],
    )
    with open("test.csv", "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def test_load_BigQuery_JSON ():
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # Note - need to exchange : for . in readme
    table_id = "bigquery-299706.201225BigQuery.Test123"
    json_rows = [{"Name": "BillyJ"},{"Name": "FredK"}]

    job = client.load_table_from_json(json_rows, table_id)
    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def test_load_BigQuery_Pandas():
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # Note - need to exchange : for . in readme
    table_id = "bigquery-299706.201225BigQuery.Test123"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Name", "STRING"),
            bigquery.SchemaField("ID", "INTEGER"),
            bigquery.SchemaField("TimestampValue", "TIMESTAMP")
            ],
    )

    data = [{"Name": "Thing1","TimestampValue":datetime.now(),"ID":5},{"Name": "Thing2","TimestampValue":datetime.now(),"ID":6}] 
    pd_dataframe = pd.DataFrame(data)
    print (pd_dataframe)

    job = client.load_table_from_dataframe(pd_dataframe, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def BigQueryQuery ():
    client = bigquery.Client()
    table_id = "bigquery-299706.201225BigQuery.Test123"

    query_string = """
                    select Name, max(TimestampValue) as TimestampValue 
                    from `bigquery-299706.201225BigQuery.Test123` 
                    where TimestampValue is not null 
                    group by Name
                    """

    print(client.query(query_string).result())
    dataframe = (
        client.query(query_string).result().to_dataframe() #bqstorage_client=bqstorageclient
    )
    print(dataframe)


def main():
    #test_login_BigQuery()
    #test_load_BigQuery()
    #test_load_BigQuery_JSON()
    test_load_BigQuery_Pandas()
    #BigQueryQuery()

if __name__ == '__main__':
    main()
