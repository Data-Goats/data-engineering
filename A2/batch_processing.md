## Overview
- Firstly, a bucket named "geitenemmer" was created in the data-goats-de group project as follows:
```
REGION=us-central1
BUCKET_NAME=geitenemmer

gsutil mb -c standard -l ${REGION} gs://${BUCKET_NAME}
```
- Secondly, the "e-shop clothing 2008.csv" data was uploaded to this bucket.
- After this, the Dataproc and BigQuery API's were enabled. 
- Then, a Dataproc cluster could be created on which the PySpark analysis was ran. 
- The results of this were then read into a BigQuery table with a matching schema.

## To set up a Dataproc cluster for the Batch Processing analysis
1. run in the shell of the Google Cloud console:
```
gcloud beta dataproc clusters create a2dataproc --project=data-goats-de --region=us-central1 --image-version=1.4 --master-machine-type=n1-standard-4 
--worker-machine-type=n1-standard-4 --bucket=geitenemmer --optional-components=ANACONDA,JUPYTER --enable-component-gateway
```

2. after this, SSH into a JupyterLab webinterface on the newly created "a2dataproc" cluster.
3. then, open up the "DataBatchCode" Python3 notebook in the GCS folder.
