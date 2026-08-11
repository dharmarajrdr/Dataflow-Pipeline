# 10. Setting Up the Postgres Pipeline

- Full setup steps live in the module's own [README](../pipelines/spanner-to-postgres/README.md), this page is a summary with the reasoning behind each step. Refer to that README for exact copy-paste commands.



### 1. Recreate the Spanner source, if needed

- If the Spanner instance from the BigQuery leg was torn down to stop billing, it needs rebuilding: instance, database, the `Orders1` table, and the `cs_orders1` change stream. Same DDL as before, all documented in the module README.



### 2. Create the Postgres target table

- A dedicated local database (`orders_cdc_poc`) and table (`orders2`), with one extra column beyond the obvious ones: `last_spanner_commit_ts`. That column exists purely as a tie-breaker in the upsert logic, so an older, retried change event can never overwrite a newer one that already landed.



### 3. Authenticate against Spanner from your laptop

- Since there's no Dataflow-managed service account involved this time (see [why, previous page](./09-postgres-pipeline-introduction.md)), the pipeline authenticates using **your own** Google Cloud identity via Application Default Credentials:

    ```bash
    gcloud auth application-default login
    gcloud auth application-default set-quota-project dharma-learn-gcp
    ```

- This is a genuinely different credential store from your regular `gcloud auth login`, having one doesn't imply the other. Both need to be set up independently.



### 4. Configure the Change Stream's value capture type

- This one surfaced as a bug during testing, not something obvious upfront, worth calling out clearly here since it'll bite anyone building a similar pipeline. Covered in full in [Troubleshooting](./12-troubleshooting-postgres-pipeline.md#the-nulled-out-columns-bug), but the short version: run this before testing an UPDATE.

    ```sql
    ALTER CHANGE STREAM cs_orders1 SET OPTIONS (value_capture_type = 'NEW_ROW');
    ```



⬅️ Back: [Introduction](./09-postgres-pipeline-introduction.md) | ➡️ Next: [Running and Verifying the Pipeline](./11-running-and-verifying-the-postgres-pipeline.md)
