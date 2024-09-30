import datetime
from io import StringIO
from pyspark.sql import SparkSession
from functools import partial
import pandas as pd
import os
from pyspark import TaskContext
from flask import Flask, render_template, request, flash, redirect, url_for
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from pyspark import StorageLevel

app = Flask(__name__)
app.secret_key = 'testing'

# Azure AD credentials

AZURE_ADLS_ACCOUNT_URL = os.getenv('AZURE_ADLS_ACCOUNT_URL')
#app.secret_key = os.getenv('SECRET_KEY')
#AZURE_CLIENT_ID

#CONTAINER_NAME = "didqsynapsefilesystem"
#FILE_PATH = "BALANCE/balance-of-payments-part8.csv"

def process_chunk(iterator, output_dir, base_filename):
    # Get the partition index
    partition_index = TaskContext.get().partitionId()

    # Convert iterator to Pandas DataFrame
    chunk_df = pd.DataFrame(list(iterator))

    # Define output file path
    output_file = os.path.join(output_dir, f"{base_filename}_partition_{partition_index}.csv")

    # Write DataFrame to CSV
    chunk_df.to_csv(output_file, index=False)
    print(f"Partition {partition_index} written to {output_file}")
    return "Done"

def spark_read_csv_file(container, latest_file, delimiter):
    TENANT_ID = os.getenv('AZURE_TENANT_ID')
    CLIENT_ID = os.getenv('AZURE_CLIENT_ID')
    CLIENT_SECRET = os.getenv('AZURE_CLIENT_SECRET')
    STORAGE_ACCOUNT_NAME = "didqynapsestorage"
    spark = SparkSession.builder \
        .appName("flask_sample") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.2.0,com.microsoft.azure:azure-storage:8.6.6") \
        .config("spark.network.timeout", "300s") \
        .config("spark.executor.instances", "3") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.driver.cores", "1") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "400m") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.metrics.enabled","true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", CLIENT_ID)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", CLIENT_SECRET)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token")
    file_path = f"abfss://{container}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{latest_file}"
    df = spark.read.option("delimiter", ",").csv(file_path, header=True, inferSchema=True)
    #df.persist(StorageLevel.DISK_ONLY)
    return spark, df
def get_data_lake_service_client():
    account_url = os.getenv('AZURE_ADLS_ACCOUNT_URL')
    if not account_url:
        raise ValueError("AZURE_ADLS_ACCOUNT_URL environment variable is not set")
    credential = DefaultAzureCredential()
    return DataLakeServiceClient(account_url=account_url, credential=credential)

def read_adls_file(container_name, file_path, flag):
    try:
        # credential = DefaultAzureCredential()
        data_lake_service_client = get_data_lake_service_client()
        file_system_client = data_lake_service_client.get_file_system_client(container_name)
        file_client = file_system_client.get_file_client(file_path)
        if flag == 'Y':
            download = file_client.download_file()
            downloaded_bytes = download.readall()
            content = downloaded_bytes.decode('utf-8')
        else:
            offset = 0
            length = 1024*1024
            download = file_client.download_file(offset=offset, length=length)
            downloaded_bytes = download.readall()
            content = downloaded_bytes.decode('utf-8')
        return content
    except Exception as e:
        return f"Error accessing ADLS Gen2: {str(e)}"

def project_and_files():
    project_files = {}
    met_file = "METADATA/metadata.csv"
    metadata = read_adls_file('metadata', met_file, 'Y')
    if "Error" in metadata:
        return metadata
    data = StringIO(metadata)
    df = pd.read_csv(data, delimiter='|')
    containers = df['CONTAINER'].sort_values().unique().tolist()
    for container in containers:
        df1 = df.loc[df['CONTAINER'] == container, ['PREFIX']]
        files = df1['PREFIX'].sort_values().unique().tolist()
        project_files[container] = files
    containers.append('')
    return containers, project_files, files

@app.route('/', methods=['GET', 'POST'])
def Home():
    return render_template('Home_Test.html')

@app.route('/action', methods=['GET', 'POST'])
def action():
    button = request.form['submit_button']
    if button == 'VALIDATE FILE':
        return redirect(url_for('f_validate'))

@app.route('/f_validate', methods=['GET', 'POST'])
def f_validate():
    containers, project_files, files = project_and_files()
    return render_template('FileValidate.html', categories=containers, subcategories=project_files)

@app.route('/file_validate', methods=['GET', 'POST'])
def file_validate():
    button = request.form['submit_button']
    if button == 'HOME':
        return redirect(url_for('Home'))
    elif button == 'VALIDATE':
        container = request.form['category']
        file = request.form['subcategory']
        directory = 'BITCOIN'
        now = datetime.datetime.now()
        print(now)
        data_lake_service_client = get_data_lake_service_client()
        file_system_client = data_lake_service_client.get_file_system_client(container)
        paths = file_system_client.get_paths(path=directory)
        all_files = [path for path in paths if not path.is_directory]
        file_dict = max(all_files, key=lambda x: x.last_modified)
        latest_file = file_dict['name']
        print(latest_file)
        spark, data_df = spark_read_csv_file(container, latest_file, ',')
        num_partitions = 10
        column_names = data_df.columns
        print(column_names)
        data_df = data_df.repartition(num_partitions)
        partial_process_partition = partial(process_chunk, output_dir='C:\\Users\\KX173ZE\\Downloads\\test', base_filename='processed_final')
        data_df.foreachPartition(partial_process_partition)
        spark.stop()
        message = 'This is done'
        now2 = datetime.datetime.now()
        print(now2)
        flash(message)
        return redirect(url_for('f_validate'))

if __name__ == "__main__":
    app.run()
    app.debug = True