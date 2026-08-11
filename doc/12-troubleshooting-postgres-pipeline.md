# 12. Troubleshooting the Postgres Pipeline

### "No Dataflow job/VM found in GCP"

- Covered in full on the [introduction page](./09-postgres-pipeline-introduction.md#why-nothing-shows-up-in-the-dataflow-console-or-compute-engine). Short version: this pipeline intentionally runs on `DirectRunner` (a local process on your laptop), not the managed Dataflow service, because Dataflow's cloud-hosted worker VMs have no network path to a Postgres instance sitting behind your laptop's home NAT. Nothing shows up in the Dataflow or Compute Engine consoles because nothing was ever launched there. The terminal running `mvn exec:java` is the only place this pipeline is observable.



### UNAUTHENTICATED: Request is missing required authentication credential

```
com.google.cloud.spanner.SpannerException: UNAUTHENTICATED:
Request is missing required authentication credential.
```

- **Cause:** Application Default Credentials (ADC) were never set up on this machine. This is a separate credential store from your regular `gcloud auth login`, having one doesn't give you the other.

- **Fix:**

    ```bash
    gcloud auth application-default login
    gcloud auth application-default set-quota-project dharma-learn-gcp
    ```

  Let the browser flow complete fully, don't `Ctrl+C` the terminal before it prints `Credentials saved to file: ...`, an interrupted login leaves no credentials behind at all.



### NoSuchMethodError: BufferRecycler.releaseToPool()

```
Caused by: java.lang.NoSuchMethodError: 'void com.fasterxml.jackson.core.util.BufferRecycler.releaseToPool()'
    at com.google.cloud.spanner.SpannerException...ObjectMapper.writeValueAsBytes
    at org.apache.beam.runners.direct.DirectRunner.run
```

- **Cause:** A classic Jackson `jackson-core`/`jackson-databind` version mismatch. `beam-sdks-java-io-google-cloud-platform` already brings in a matched pair of these transitively, and Beam's `DirectRunner` depends on that exact pairing internally (it serializes `PipelineOptions` to JSON on startup). The pipeline's `pom.xml` originally pinned an explicit, newer `jackson-databind` version for our own JSON parsing code, which silently overrode just the `databind` half of that pair, leaving a mismatched `jackson-core` underneath.

- **Fix:** Remove the explicit `jackson-databind` dependency from `pom.xml` entirely and let Beam's transitive version resolve consistently. Our own parsing code (`ObjectMapper`/`JsonNode` in `DataChangeRecordToOrderChangeFn.java`) needs no changes, those classes just come from Beam's bundled copy instead. Run `mvn clean` afterward to force re-linking rather than reusing a stale local build.



### Logs going completely silent (no LOG.info output at all)

```
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation.
```

- **Cause:** Another version mismatch, this time between SLF4J's API and its logging provider. Beam pulls in the **1.7.x-line SLF4J API** transitively, which looks for a class called `StaticLoggerBinder` to bind to a concrete logger. `slf4j-simple` version `2.0.13` is a **2.x-line provider**, and 2.x providers don't implement `StaticLoggerBinder` at all, so no binding is ever found and SLF4J quietly falls back to doing nothing.

- **Fix:** Pin `slf4j-simple` to the matching `1.7.x` line instead:

    ```xml
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-simple</artifactId>
        <version>1.7.36</version>
        <scope>runtime</scope>
    </dependency>
    ```

- Worth knowing: this bug is purely cosmetic. The pipeline itself keeps running correctly even with silent logging, it's easy to mistake "no output" for "nothing is happening" and go looking for a crash that isn't there. If you're ever unsure whether it's actually alive, skip the logs and just check Postgres directly.



### The Nulled-Out Columns Bug

- This was the most subtle issue, and the one most worth understanding rather than just copy-pasting the fix.

- **Symptom:** After an `INSERT`, everything looked right in `orders2`. After an `UPDATE` that only changed `Status` and `Amount`, the row in Postgres updated, but `customer_name` and `order_date` came back **empty**, even though the `UPDATE` statement in Spanner never touched them.

    ![](../.assets/Postgres%20Update%20Nulls%20Bug.png)

- **Cause:** Spanner change streams have a setting called `value_capture_type`, and it defaults to `OLD_AND_NEW_VALUES`. Under that default, an UPDATE's change record only includes the columns that were **actually modified** in that specific statement, `CustomerName` and `OrderDate` weren't just unchanged in the JSON payload, they were **missing from it entirely**. This pipeline's `DataChangeRecordToOrderChangeFn` parses missing fields as `null`, and the upsert in `PostgresWriterFn` writes every field it's given, including those nulls, blindly overwriting the previously-good values.

  > This is exactly the same tradeoff the BigQuery template's own documentation flags: under the default capture type, a consumer needs to do an extra "stale read" back to Spanner to recover unmodified columns, or configure the change stream differently so the full row is always included. The Google-managed BigQuery template handles the stale read internally; this hand-written pipeline doesn't, so it needed the second option instead.

- **Fix:** Switch the change stream's capture type so every INSERT/UPDATE event always carries the **complete current row**, not just the delta:

    ```sql
    ALTER CHANGE STREAM cs_orders1 SET OPTIONS (value_capture_type = 'NEW_ROW');
    ```

    ![](../.assets/Change%20Stream%20Set%20NEW_ROW.png)

- This is a server-side setting change, no pipeline code needed updating, and no restart was strictly required, though restarting for a clean test is good practice. Retesting the exact same UPDATE afterward confirmed the fix:

    ![](../.assets/Postgres%20Update%20Preserves%20Fields.png)

  `customer_name` and `order_date` now survive an UPDATE that never touched them, only the columns that actually changed (`amount`, `status`) reflect the new values.

- **Takeaway for the eventual Oracle pipeline:** this same `NEW_ROW` setting will be just as necessary there. Any JDBC-based sink that does upsert-by-full-row (rather than a targeted `SET column = value` per changed field) needs the complete row on every event, not just the delta.



⬅️ Back: [Running and Verifying the Pipeline](./11-running-and-verifying-the-postgres-pipeline.md) | ⬆️ Back to [Guide Index](../README.md)
