# 6. Setting Up the Source, Target, and Permissions

- Unlike the GCS → Spanner pipeline, this one has a few more moving pieces since it's a streaming job. Let's set each one up in order: source, target, staging, and finally permissions.



### Setting Up the Source — Cloud Spanner

- Go to [Cloud Spanner](https://console.cloud.google.com/spanner/instances) and create a new instance.
    - For this POC, `us-central1 (Iowa)` was picked over an `asia-south1` region purely because it's cheaper to run. Region choice doesn't affect the steps below, pick whatever fits your budget.
    - Standard edition is fine for a POC, no need for Enterprise.

- Open [Spanner Studio](https://console.cloud.google.com/spanner/instances/spanner-instance/databases/spanner-database/details/query) for your new database and create the source table:

    ```sql
    CREATE TABLE Orders1 (
      OrderId       STRING(36) NOT NULL,
      CustomerName  STRING(100),
      OrderDate     DATE,
      Amount        NUMERIC,
      Status        STRING(20),
    ) PRIMARY KEY (OrderId);
    ```

- Now enable a Change Stream on it. This is the part that turns a normal table into something Dataflow can watch live:

    ```sql
    CREATE CHANGE STREAM cs_orders1
    FOR Orders1;
    ```

- Run these as separate statements. Once the second one succeeds, you'll see `cs_orders1` listed under **Change Streams** in the left-hand explorer of Spanner Studio.

    ![](../.assets/Spanner%20Studio%20CDC%20Script.png)

    > The screenshot above also shows the full test script we'll use later to insert, update, and delete a row, plus the `Change streams 1` entry confirming it's active.



### Setting Up the Target — BigQuery

- The Dataflow template will create the destination **table** for you automatically, but the **dataset** has to exist first.

    ```shell
    bq --location="us-central1" mk \
      --dataset \
      --description "CDC POC - Spanner Orders1 to BigQuery" \
      "dharma-learn-gcp:spanner_cdc_poc"
    ```

- One naming quirk worth knowing upfront: the template doesn't write to a table named exactly like your source (`Orders1`). It appends `_changelog` to it, so you'll end up querying `Orders1_changelog`, not `Orders1`. This is expected, not a bug, we'll cover why in the next doc.



### Setting Up Staging — GCS Bucket

- Dataflow needs somewhere to stage its binaries and hold temp files while the job runs.

    ```shell
    gcloud storage buckets create "gs://dharma-learn-gcp-df-cdc-poc" \
      --project="dharma-learn-gcp" \
      --location="us-central1" \
      --uniform-bucket-level-access
    ```



### Setting Up Permissions — IAM

- This is the step that's easy to miss on a fresh project. Dataflow workers run using your project's default compute service account, and that account has **no access to Spanner or BigQuery** until you grant it explicitly.

- Find your worker service account first (it's named using your project number, not your project ID):

    ```shell
    export PROJECT_NUMBER=$(gcloud projects describe "dharma-learn-gcp" --format="value(projectNumber)")
    export WORKER_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
    echo "$WORKER_SA"
    ```

- Then grant it everything it needs to read the change stream, write to BigQuery, and use the staging bucket:

    ```shell
    gcloud projects add-iam-policy-binding "dharma-learn-gcp" \
      --member="serviceAccount:${WORKER_SA}" \
      --role="roles/spanner.databaseUser"

    gcloud projects add-iam-policy-binding "dharma-learn-gcp" \
      --member="serviceAccount:${WORKER_SA}" \
      --role="roles/bigquery.dataEditor"

    gcloud projects add-iam-policy-binding "dharma-learn-gcp" \
      --member="serviceAccount:${WORKER_SA}" \
      --role="roles/bigquery.jobUser"

    gcloud projects add-iam-policy-binding "dharma-learn-gcp" \
      --member="serviceAccount:${WORKER_SA}" \
      --role="roles/storage.objectAdmin"

    gcloud projects add-iam-policy-binding "dharma-learn-gcp" \
      --member="serviceAccount:${WORKER_SA}" \
      --role="roles/dataflow.worker"
    ```

- With source, target, staging, and permissions all in place, we're ready to actually launch the pipeline.



⬅️ Back: [Introduction to Change Streams](./05-change-streams-introduction.md) | ➡️ Next: [Running and Verifying the Pipeline](./07-running-and-verifying-the-pipeline.md)
