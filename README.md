# Kestra Google Cloud Platform Tasks

<p align="center">
  <img width="460" src="https://github.com/kestra-io/kestra/raw/master/ui/src/assets/logo.svg?sanitize=true"  alt="Kestra workflow orchestrator" />
</p>

> Tasks to interract with Google Cloud Platform

## Task

### BigQuery
* `org.kestra.task.gcp.bigquery.Load`: Load files into Bigquery  
* `org.kestra.task.gcp.bigquery.LoadFromGcs`: Load files from GCS into Bigquery
* `org.kestra.task.gcp.bigquery.Query`: Send a job query to bigquery 

### BigQuery Dataset
* `org.kestra.task.gcp.bigquery.CreateDataset`: Create a new Bigquery dataset
* `org.kestra.task.gcp.bigquery.DeleteDataset`: Delete a new Bigquery dataset
* `org.kestra.task.gcp.bigquery.UpdateDataset`: Update a new Bigquery dataset

### Cloud Storage
* `org.kestra.task.gcp.gcs.Copy`: Copy files between buckets & buckets directory 
* `org.kestra.task.gcp.gcs.Upload`: Upload files to bucket 
* `org.kestra.task.gcp.gcs.Download`: Download files from bucket 

### Cloud Storage Bucket
* `org.kestra.task.gcp.gcs.CreateBucket`: Create a new Cloud Storage bucket
* `org.kestra.task.gcp.gcs.DeleteBucket`: Delete a new Cloud Storage bucket
* `org.kestra.task.gcp.gcs.UpdateBucket`: Update a new Cloud Storage bucket

## License
Apache 2.0 © [Nigh Tech](https://nigh.tech)
