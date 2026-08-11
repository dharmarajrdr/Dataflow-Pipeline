# Spanner Change Streams → Postgres (JDBC sink POC)

- This module is a stand-in for the real end goal, **Spanner Change Streams → Oracle**. Oracle isn't installed on this laptop, and Postgres is, so we're using Postgres to learn and validate the pattern first. Both are relational databases reachable over JDBC, so the pipeline code here (parsing change records, deciding upsert vs. delete, writing via `PreparedStatement`) carries over to Oracle almost unchanged, only the JDBC driver and SQL dialect change.

- Unlike the BigQuery leg of this guide, there's no Google-provided template for this. `Spanner_to_SourceDb` looked promising at first (it's literally called "reverse replication"), but it turned out to be built specifically for sharded MySQL targets, not generic JDBC. So this module is a small hand-written Apache Beam pipeline instead.



### How this differs from the BigQuery pipeline

| | BigQuery leg | This module |
|---|---|---|
| Write behavior | Append-only changelog, every event is a new row | Real upsert/delete, table reflects *current* state |
| Runner | Managed Dataflow service, in the cloud | `DirectRunner`, runs locally on your laptop |
| Why | BigQuery isn't built for row rewrites | Postgres (and Oracle) are exactly built for this |

- The `DirectRunner` choice isn't just for convenience. The managed Dataflow *service* runs worker VMs inside Google's cloud, they have no path to reach a Postgres instance sitting behind your laptop's home network/NAT. `DirectRunner` runs the whole pipeline as a regular local process instead, which can reach `localhost:5432` just fine, and it's free, with a much faster feedback loop for learning than waiting on cloud workers each run.



### 1. Recreate the Spanner source (skip if it already exists)

- If you tore down the instance/database from the BigQuery leg to stop billing, you'll need to rebuild the source side first. Same commands as before, repeated here so this module is self-contained.

    ```bash
    gcloud spanner instances create spanner-instance \
      --project=dharma-learn-gcp \
      --config=regional-us-central1 \
      --description="CDC POC Instance" \
      --edition=STANDARD \
      --processing-units=100
    ```

    ```bash
    gcloud spanner databases create spanner-database \
      --project=dharma-learn-gcp \
      --instance=spanner-instance
    ```

- Then open [Spanner Studio](https://console.cloud.google.com/spanner/instances/spanner-instance/databases/spanner-database/details/query) and recreate the source table and change stream, as two separate DDL statements:

    ```sql
    CREATE TABLE Orders1 (
      OrderId       STRING(36) NOT NULL,
      CustomerName  STRING(100),
      OrderDate     DATE,
      Amount        NUMERIC,
      Status        STRING(20),
    ) PRIMARY KEY (OrderId);
    ```

    ```sql
    CREATE CHANGE STREAM cs_orders1
    FOR Orders1;
    ```

- Quick sanity check that both exist before moving on:

    ```bash
    gcloud spanner databases ddl describe spanner-database \
      --project=dharma-learn-gcp \
      --instance=spanner-instance
    ```

  You should see both the `CREATE TABLE Orders1` and `CREATE CHANGE STREAM cs_orders1` statements in the output.

- One thing worth remembering from last time: this instance bills continuously while it exists, whether or not you're actively using it. Once you're done testing this module, tear it down again with `gcloud spanner instances delete spanner-instance --project=dharma-learn-gcp` if you don't need it running.



### 2. Create the target table in Postgres

```bash
psql postgres
```

```sql
CREATE DATABASE orders_cdc_poc;
\c orders_cdc_poc

CREATE TABLE orders2 (
    order_id                 VARCHAR(36) PRIMARY KEY,
    customer_name             VARCHAR(100),
    order_date                DATE,
    amount                     NUMERIC,
    status                     VARCHAR(20),
    last_spanner_commit_ts    VARCHAR(50)
);
```

- `last_spanner_commit_ts` is stored as text (Spanner's `Timestamp.toString()` output) rather than a native timestamp column. It's used purely as a tie-breaker in the upsert's `WHERE` clause, to avoid an older retried event overwriting a newer one, string comparison works fine for that since the format is fixed-width and lexically sortable.



### 3. Authenticate against Spanner from your laptop

- This pipeline talks to Spanner directly using your own Google Cloud credentials, not a service account baked into a Dataflow job. If you haven't already:

    ```bash
    gcloud auth application-default login
    ```

- Make sure your account has read access to the change stream (`roles/spanner.databaseReader` or better on `dharma-learn-gcp`, you already have `roles/owner` from earlier, so this is covered).



### 4. Run the pipeline

```bash
cd pipelines/spanner-to-postgres

mvn compile exec:java \
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

- Adjust `postgresUsername`/`postgresPassword` if your local Postgres role differs (yours connects as `dharmaraj` with no password, based on the earlier `psql postgres` session).

- This process runs in the foreground and keeps streaming until you stop it with `Ctrl+C`, same "runs forever" nature as the Dataflow-hosted BigQuery job, just running on your machine instead of in the cloud.

- You should see log lines like:

    ```
    [main] INFO  c.d.d.cdc.PostgresWriterFn - Postgres connection opened: jdbc:postgresql://localhost:5432/orders_cdc_poc
    ```

  confirming it connected and is now waiting for change stream events.



### 5. Test it end-to-end

- With the pipeline running, go to Spanner Studio and run the same test script from the BigQuery leg:

    ```sql
    INSERT INTO Orders1 (OrderId, CustomerName, OrderDate, Amount, Status)
    VALUES ('order-001', 'Dharmaraj', '2026-08-11', 4999.00, 'PLACED');

    UPDATE Orders1
    SET Status = 'SHIPPED', Amount = 5499.00
    WHERE OrderId = 'order-001';

    DELETE FROM Orders1 WHERE OrderId = 'order-001';
    ```

- Watch the terminal running the pipeline, you should see three log lines fire in near real-time, one per statement (`INSERT`, `UPDATE`, `DELETE`).

- Check Postgres directly to confirm the *current state* behavior:

    ```bash
    psql orders_cdc_poc -c "SELECT * FROM orders2;"
    ```

- After the `INSERT`, you'll see one row. After the `UPDATE`, the **same row** updates in place (no second row, unlike the BigQuery changelog). After the `DELETE`, the row disappears entirely. This is the key behavioral difference from the BigQuery leg, and it's exactly the behavior a real operational Oracle database would need too.



### Known limitations (fine for a POC, not for production)

- One JDBC connection per worker, opened once and reused. No connection pooling. For production, swap in a pooled `DataSource` (e.g., HikariCP).
- No dead-letter handling, malformed records are logged and dropped rather than retried or queued elsewhere.
- `DirectRunner` isn't horizontally scalable and isn't meant to be, it's for local development and learning. The Oracle version of this, when it eventually runs against real infrastructure, would move to the Dataflow-managed `FlexRSRunner`/`DataflowRunner` with proper network connectivity (VPC peering or a Cloud SQL Auth Proxy-equivalent) between the Dataflow workers and the Oracle instance.
