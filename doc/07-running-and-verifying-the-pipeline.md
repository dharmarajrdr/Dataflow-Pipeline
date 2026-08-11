# 7. Running and Verifying the Pipeline

- This template doesn't have a simple UI flow like the batch one did, since it needs a metadata database for checkpointing. We'll launch it with `gcloud` instead.



### Launching the Job

```shell
gcloud dataflow flex-template run "spanner-cdc-to-bq-poc" \
  --project="dharma-learn-gcp" \
  --region="us-east1" \
  --template-file-gcs-location="gs://dataflow-templates-us-east1/latest/flex/Spanner_Change_Streams_to_BigQuery" \
  --staging-location="gs://dharma-learn-gcp-df-cdc-poc/staging" \
  --temp-location="gs://dharma-learn-gcp-df-cdc-poc/temp" \
  --num-workers=2 \
  --additional-experiments=use_runner_v2,enable_streaming_engine \
  --parameters \
spannerInstanceId="spanner-instance",\
spannerDatabase="spanner-database",\
spannerChangeStreamName="cs_orders1",\
bigQueryDataset="spanner_cdc_poc",\
spannerMetadataInstanceId="spanner-instance",\
spannerMetadataDatabase="spanner-database"
```

- A couple of parameters worth explaining:
    - `spannerMetadataInstanceId` / `spannerMetadataDatabase`: Dataflow needs somewhere to track its own read progress through the change stream (checkpointing), so it doesn't reread the same changes twice or miss any after a restart. We just pointed it at the same instance and database as the source, the template creates its own internal metadata table there automatically.
    - `--region` here is the **Dataflow job's** region, it doesn't have to match the Spanner instance's region. We ended up running Dataflow in `us-east1` while Spanner stayed in `us-central1`, more on why in the [troubleshooting doc](./08-troubleshooting-cdc-pipeline.md).

- Unlike the batch job, this one runs forever (or until you stop it), continuously watching for changes. That's the nature of a streaming pipeline.



### Confirming It's Actually Running

- Check the job's state directly:

    ```shell
    gcloud dataflow jobs list --region="us-east1"
    ```

- Look for `Streaming` as the type and `Running` as the state.

- Head to the [Dataflow Jobs console](https://console.cloud.google.com/dataflow/jobs) and open the job. This time, unlike a failed launch, the **Job Graph** actually renders a real pipeline diagram:

    ![](../.assets/CDC%20Job%20Graph%20Running.png)

    > Each box is a stage: reading from the change stream, reshuffling records, converting them to BigQuery-friendly JSON, and finally writing them. The `Job info` panel on the right also confirms `Streaming Mode: Exactly once` and `Streaming Engine: Enabled`, both of which matter for not losing or duplicating any change events.

- You can also check [Compute Engine](https://console.cloud.google.com/compute/instances) to see the actual worker VMs doing the processing:

    ![](../.assets/CDC%20Worker%20VMs.png)



### Testing the Pipeline End-to-End

- With the job running, go back to Spanner Studio and insert a row into `Orders1`:

    ```sql
    INSERT INTO Orders1 (OrderId, CustomerName, OrderDate, Amount, Status)
    VALUES ('order-001', 'Dharmaraj', '2026-08-11', 4999.00, 'PLACED');
    ```

- Give it about 30-60 seconds (first write needs the pipeline to create the BigQuery table's schema, which takes a little longer than later writes), then query BigQuery. Remember, the table name has `_changelog` appended:

    ```sql
    SELECT
      OrderId,
      CustomerName,
      OrderDate,
      Amount,
      Status,
      _metadata_spanner_mod_type,
      _metadata_spanner_commit_timestamp,
      _metadata_big_query_commit_timestamp
    FROM
      `dharma-learn-gcp.spanner_cdc_poc.Orders1_changelog`
    ORDER BY
      _metadata_spanner_commit_timestamp DESC
    LIMIT 10;
    ```

    ![](../.assets/BigQuery%20Insert%20Only%20Result.png)

- Now let's prove this is genuinely tracking changes live, not just a one-time copy. Run an update and a delete back in Spanner Studio:

    ```sql
    UPDATE Orders1
    SET Status = 'SHIPPED', Amount = 5499.00
    WHERE OrderId = 'order-001';

    DELETE FROM Orders1 WHERE OrderId = 'order-001';
    ```

- Re-run the same BigQuery query after another 30-60 seconds:

    ![](../.assets/BigQuery%20Changelog%20Result.png)

- Three rows show up for the same `OrderId`, one per event:

    | Mod Type | What it shows |
    |---|---|
    | `INSERT` | The original row, full values captured |
    | `UPDATE` | The new `Status`/`Amount`, captured as a fresh row |
    | `DELETE` | Only `OrderId` is populated, every other column is `null` |

    > The `DELETE` row only carrying the primary key isn't a bug, by the time the change stream record is processed, the row is already gone from Spanner, so there's nothing left to read except the key that identified it.



### Why BigQuery Keeps Every Version Instead of Just the Latest

- It's tempting to expect BigQuery to just show the "current" state of the row, like Spanner does. It doesn't, and that's deliberate:

    - **BigQuery isn't built for row-level rewrites.** It's a columnar, append-optimized warehouse. Streaming inserts are cheap; doing a `MERGE`/upsert per change event at scale is not.
    - **The whole point of Change Data Capture is the history, not just the snapshot.** An append-only changelog gives you an audit trail, the ability to reconstruct state at any point in time, and the ability to react to specific transitions (like "notify when Status becomes SHIPPED").
    - **Getting the latest state back is easy, going backwards isn't.** If you want a "current view," you can always collapse the changelog with a query. But if you'd stored only the latest state, the history would already be gone, there'd be no way to recover it.

- Here's the query to collapse the changelog down to current state per `OrderId`, useful for any downstream consumer that only cares about "now":

    ```sql
    SELECT * EXCEPT(rn) FROM (
      SELECT *,
        ROW_NUMBER() OVER (
          PARTITION BY OrderId
          ORDER BY _metadata_spanner_commit_timestamp DESC
        ) AS rn
      FROM `dharma-learn-gcp.spanner_cdc_poc.Orders1_changelog`
      WHERE _metadata_spanner_mod_type != 'DELETE'
    )
    QUALIFY rn = 1;
    ```

- This distinction matters a lot for the Oracle target we'll build later: Oracle is typically expected to hold live, current-state data (like Spanner does), not an event log. So the Oracle sink pipeline will need real upsert/delete logic keyed off `_metadata_spanner_mod_type`, rather than blindly appending like this BigQuery template does.



⬅️ Back: [Setting up the Source and Target](./06-setting-up-the-cdc-pipeline.md) | ➡️ Next: [Troubleshooting](./08-troubleshooting-cdc-pipeline.md)
