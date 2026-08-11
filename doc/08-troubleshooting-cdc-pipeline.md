# 8. Troubleshooting the Change Streams Pipeline

- Getting this pipeline running end-to-end took a few failed attempts first. Here's every issue that came up, in the order it actually happened, so you can recognize them faster than we did.

    ![](../.assets/CDC%20Jobs%20List.png)

    > Four attempts total before one stuck: three straight-up `Failed`, one that eventually ran. Every job here shares the same name, `spanner-cdc-to-bq-poc`, since a fresh launch gets a brand new Job ID each time.



### Issue 1 — Unrecognized Template Parameter

- The very first launch attempt included a parameter called `gcsUtilityOutputFileDirectory`, copied in from an assumption about what the template might need.

    ```
    ERROR: (gcloud.dataflow.flex-template.run) INVALID_ARGUMENT: The template parameters are invalid. Details:
    gcsUtilityOutputFileDirectory: Unrecognized parameter
    ```

- **Fix:** Don't guess parameter names. This template's actual required parameters are just: `spannerInstanceId`, `spannerDatabase`, `spannerChangeStreamName`, `bigQueryDataset`, `spannerMetadataInstanceId`, `spannerMetadataDatabase`. Drop anything not on that list.



### Issue 2 — Zone Resource Pool Exhausted

- The next two attempts failed within 9-10 seconds, before the job graph even built:

    ![](../.assets/Change%20Stream%20Job%20Failed.png)

    > Note the **"The graph is unavailable"** message. That's a strong signal the job never made it past the launch stage, worth checking the logs immediately rather than waiting.

- Pulling the logs showed the actual cause:

    ```shell
    gcloud logging read \
      'resource.type="dataflow_step" AND resource.labels.job_id="YOUR_JOB_ID" AND severity>=ERROR' \
      --project="dharma-learn-gcp" \
      --limit=20 \
      --format="value(textPayload)"
    ```

    ```
    Failed to start the launcher VM ... [ZONE_RESOURCE_POOL_EXHAUSTED]
    'The zone 'projects/dharma-learn-gcp/zones/us-central1-a' does not have
    enough resources available to fulfill the request.'
    ```

- The natural instinct is to pin a different zone with `--worker-zone`. That didn't help here, the **launcher VM** (used only to bootstrap the flex template before handing off to actual workers) gets pinned to your region's `-a` zone automatically, regardless of `--worker-zone`.

- **Fix:** Since the launcher zone can't be overridden directly, the workaround is moving the whole Dataflow job to a different **region** instead. Spanner stayed in `us-central1`; Dataflow moved to `us-east1`:

    ```shell
    gcloud dataflow flex-template run "spanner-cdc-to-bq-poc" \
      --region="us-east1" \
      --template-file-gcs-location="gs://dataflow-templates-us-east1/latest/flex/Spanner_Change_Streams_to_BigQuery" \
      ...
    ```

    > This adds a small amount of cross-region latency and egress cost between Dataflow and Spanner. Fine for a POC; for production, you'd want to retry the same region rather than permanently split them, since these capacity shortages are usually short-lived.



### Issue 3 — Permission Denied Creating Spanner Sessions

- With the region fixed, the job launched and reached `Running`, but no data ever showed up downstream. Checking the launcher logs revealed why:

    ```
    com.google.cloud.spanner.SpannerException: PERMISSION_DENIED:
    Operation denied by [IAM permission 'spanner.sessions.create' on resource
    '//spanner.googleapis.com/projects/dharma-learn-gcp/instances/spanner-instance/databases/spanner-database'].
    ```

- This is expected on a fresh project. The Dataflow worker service account has zero access to your data services until you grant it, it doesn't inherit anything just by being the "default" compute account.

- **Fix:** Grant the roles shown in [doc 6](./06-setting-up-the-cdc-pipeline.md#setting-up-permissions--iam) before launching. You can always check what's currently bound with:

    ```shell
    gcloud projects get-iam-policy dharma-learn-gcp \
      --filter="bindings.members:474421665985-compute@developer.gserviceaccount.com"
    ```

- If that returns `Listed 0 items.`, the service account has no roles at all, that's your answer right there.



### Issue 4 — "Table Not Found" After a Successful Insert

- Once permissions were fixed and the job was genuinely processing data, querying `Orders1` in BigQuery still failed:

    ```
    Not found: Table dharma-learn-gcp:spanner_cdc_poc.Orders1 was not found in location us-central1
    ```

- **Fix:** This wasn't a real failure, just a wrong table name assumption. The template automatically appends `_changelog` to the source table name when creating the BigQuery table. Confirm what actually got created with:

    ```shell
    bq ls "dharma-learn-gcp:spanner_cdc_poc"
    ```

- You'll see `Orders1_changelog`, not `Orders1`. Query that instead. The same applies to sorting, the timestamp column is `_metadata_spanner_commit_timestamp`, not a generic `_metadata_timestamp`, always confirm actual column names with `bq show --schema` rather than guessing.



### General Debugging Approach That Worked

1. Check `gcloud dataflow jobs list --region=REGION` first, is it `Running`, `Failed`, or stuck in `Queued`?
2. If `Failed` fast (under ~15 seconds), the job never really started, pull launcher logs.
3. If `Running` but no data appears downstream, check for `PERMISSION_DENIED` in the logs before assuming the pipeline logic is wrong.
4. If a query fails with "table/column not found," confirm actual names with `bq ls` / `bq show --schema` rather than assuming the template matches your source naming exactly.



⬅️ Back: [Running and Verifying the Pipeline](./07-running-and-verifying-the-pipeline.md) | ⬆️ Back to [Guide Index](../README.md)
