# 11. Running and Verifying the Postgres Pipeline

### Launching it

```bash
cd pipelines/spanner-to-postgres

mvn clean compile exec:java \
  -Dexec.mainClass=com.dharma.dataflow.cdc.SpannerChangeStreamToPostgresPipeline \
  -Dexec.args="\
--spannerProjectId=dharma-learn-gcp \
--spannerInstanceId=spanner-instance \
--spannerDatabaseId=spanner-database \
--changeStreamName=cs_orders1 \
--metadataInstanceId=spanner-instance \
--metadataDatabaseId=spanner-database \
--postgresJdbcUrl=jdbc:postgresql://localhost:5432/orders_cdc_poc \
--postgresUsername=dharmaraj \
--postgresPassword="
```

- This runs in the foreground of your terminal and keeps streaming until `Ctrl+C`. As covered on the [introduction page](./09-postgres-pipeline-introduction.md), there's no job to go look at in the Dataflow console, this terminal window **is** the running pipeline.



### Testing INSERT → UPDATE → DELETE

- With the pipeline running, this script in Spanner Studio exercises all three change types:

    ```sql
    INSERT INTO Orders1 (OrderId, CustomerName, OrderDate, Amount, Status)
    VALUES ('order-001', 'Dharmaraj', '2026-08-11', 4999.00, 'PLACED');

    UPDATE Orders1
    SET Status = 'SHIPPED', Amount = 5499.00
    WHERE OrderId = 'order-001';

    DELETE FROM Orders1 WHERE OrderId = 'order-001';
    ```

- After the `INSERT`, checking Postgres shows the full row landing correctly:

    ```bash
    psql orders_cdc_poc -c "SELECT * FROM orders2;"
    ```

    ```
     order_id  | customer_name | order_date | amount | status |      last_spanner_commit_ts
    -----------+---------------+------------+--------+--------+-----------------------------------
     order-001 | Dharmaraj     | 2026-08-11 |   4999 | PLACED | 2026-08-11T18:32:57.907663000Z
    ```

- After the `UPDATE`, with the change stream's `value_capture_type` set to `NEW_ROW` (see [Troubleshooting](./12-troubleshooting-postgres-pipeline.md#the-nulled-out-columns-bug) for why that matters), the row updates **in place**, unchanged columns stay intact:

    ![](../.assets/Postgres%20Update%20Preserves%20Fields.png)

    > `customer_name` and `order_date` are still there even though the `UPDATE` statement never touched them. Only `amount` and `status` changed, exactly matching what actually changed in Spanner.

- After the `DELETE`, the row is gone entirely:

    ![](../.assets/Postgres%20Delete%20Result.png)



### Confirming the behavioral difference from BigQuery

| | BigQuery leg | This module |
|---|---|---|
| After INSERT | 1 row | 1 row |
| After UPDATE | **2 rows** (old + new, changelog) | **Still 1 row** (updated in place) |
| After DELETE | **3 rows** (insert + update + delete events, all kept) | **0 rows** (actually removed) |

- This is exactly the design goal from the [BigQuery leg's discussion](./07-running-and-verifying-the-pipeline.md#why-bigquery-keeps-every-version-instead-of-just-the-latest): BigQuery deliberately keeps a full event history, while an operational database like Postgres (and eventually Oracle) is expected to reflect *current* state only. Same source, same change stream, two very different, and both correct, target behaviors.



⬅️ Back: [Setting up the Postgres Pipeline](./10-setting-up-the-postgres-pipeline.md) | ➡️ Next: [Troubleshooting](./12-troubleshooting-postgres-pipeline.md)
