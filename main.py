from google.cloud import storage
from google.cloud import bigquery
from google.cloud import bigquery_storage
import pandas as pd
import pytz
import pyarrow
from datetime import datetime
import os


def main():
    # TODO Change cloudfunctiontest.json to the name of your service account credential file
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "cloudfunctiontest.json" 
    # TODO Change the table_id to table ID you coppied from BigQuery. Note - you will need to replace the : with .
    # Project_ID:Dataset.Table -> Project_ID.Dataset.Table
    table_id = "cloudfunctiontest-299816.CloudFunctionDataset.CloudFunctionTable"
    
    test_load_BigQuery_JSON(table_id)
    #test_load_BigQuery_csv(table_id)
    #test_load_BigQuery_Pandas(table_id)

    BigQueryQuery(table_id)

def test_load_BigQuery_JSON (table_id):
    # GCP Documentation - https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json
    # Construct a BigQuery client object.
    client = bigquery.Client()

    json_rows = [{"Name": "BillyJ"},{"Name": "FredK"}]

    job = client.load_table_from_json(json_rows, table_id)
    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def test_load_BigQuery_csv (table_id):
    #GCP Documentation -  https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
    # Construct a BigQuery client object.
    client = bigquery.Client()
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True, 
        schema=[bigquery.SchemaField("Name", "STRING")],
    )
    with open("names.csv", "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    
def test_load_BigQuery_Pandas(table_id):

    client = bigquery.Client() #credentials=JSON_credentials

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

def BigQueryQuery (table_id):
    
    client = bigquery.Client()

    # query to pull out the latest Name added
    query_string = f"""
                    select Name, max(TimestampValue) as TimestampValue 
                    from `{table_id}` 
                    where TimestampValue is not null 
                    group by Name
                    """

    print(client.query(query_string).result())
    dataframe = (
        client.query(query_string).result().to_dataframe() #bqstorage_client=bqstorageclient
    )
    print(dataframe)


def Cloud_Function_load_BigQuery(request_value):
    # TODO Change the table_id to table ID you coppied from BigQuery. Note - you will need to replace the : with .
    # Project_ID:Dataset.Table -> Project_ID.Dataset.Table
    table_id = "cloudfunctiontest-299816.CloudFunctionDataset.CloudFunctionTable"
    
    client = bigquery.Client() #credentials=JSON_credentials

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

if __name__ == '__main__':
    main()
