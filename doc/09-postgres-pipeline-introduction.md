# 9. Introduction to Spanner Change Streams → Postgres (Oracle Stand-in)

- The BigQuery leg of this guide used a **Google-provided template**, fill in some parameters, hit run, done. This leg is different: there's no ready-made template for a generic relational JDBC target like Postgres or Oracle, so we hand-wrote a small Apache Beam pipeline instead. The code lives in [`pipelines/spanner-to-postgres`](../pipelines/spanner-to-postgres).

- Why Postgres, when the actual end goal is Oracle? Oracle isn't installed on this laptop, and Postgres is. Both are relational databases reachable over JDBC, and the pipeline logic (parse a change record, decide upsert vs. delete, write it via `PreparedStatement`) is identical either way, only the JDBC driver and SQL dialect change. Postgres here is a stand-in to validate the pattern before pointing the same code at real Oracle infrastructure.



### Why this is a custom pipeline, not another template

- The obvious first guess was `Spanner_to_SourceDb`, a Google template literally called "reverse replication," which sounded exactly like PayPal's Cloud Modernization use case. Digging into it further, it turned out to be purpose-built for **sharded MySQL** targets specifically, it expects a `sourceShardsFilePath` describing MySQL shard topology, not a generic JDBC connection. It doesn't apply here.

- So this pipeline reads the change stream directly using Beam's `SpannerIO.readChangeStream()` (the same connector the BigQuery template uses internally), and writes to the target using plain JDBC `PreparedStatement`s with real upsert/delete logic.



### Why nothing shows up in the Dataflow console or Compute Engine

- This is worth being explicit about, since it looks like something went wrong if you go looking for it. **It's expected, and it's a deliberate choice, not a mistake.**

- The BigQuery leg ran on the **managed Dataflow service**: you launched a Flex Template, Google spun up real Compute Engine VMs as workers in your project, and the whole thing showed up under **Dataflow → Jobs** and **Compute Engine → VM instances** because it was genuinely running inside Google Cloud.

- This leg runs on Beam's **`DirectRunner`** instead, a runner that executes the entire pipeline as an ordinary local Java process, using `mvn compile exec:java` on your own laptop. There's a concrete reason it has to be this way:

    ```
    Managed Dataflow service (workers live inside Google Cloud)
                │
                ✕   <-- no path to reach this
                │
    Your laptop's Postgres (localhost:5432, behind your home NAT/firewall)
    ```

  A managed Dataflow worker VM has no network route to a database sitting behind your laptop's home router. It has no public IP, no VPN, no VPC peering set up, nothing for a cloud-hosted VM to dial into. `DirectRunner`, by contrast, runs on the same machine as Postgres itself, so `localhost:5432` is trivially reachable.

- So here's the actual flow for this leg:

    ```
    Spanner (Orders1 table, in Google Cloud)
          │
          ▼
    Change Stream (cs_orders1)
          │
          ▼   (read over the network via SpannerIO.readChangeStream(),
          │    authenticated using your own gcloud ADC credentials)
          ▼
    Beam Pipeline running as a plain Java process on YOUR laptop
    (via `mvn compile exec:java`, using DirectRunner)
          │
          ▼   (written over a local JDBC connection)
          ▼
    Postgres (orders2 table, also on your laptop, localhost:5432)
    ```

- The only two things that ever leave your laptop and touch Google Cloud are: (1) reading change stream records from Spanner, and (2) the Application Default Credentials used to authenticate that read. Everything else, the pipeline logic, the JDBC write, the Postgres database, stays entirely local. That's why there's no Dataflow job, no worker VM, and nothing billable beyond the Spanner instance itself.

- The only place you can actually observe this pipeline running is the terminal where you launched `mvn exec:java`, that terminal *is* the "worker."

- When the real Oracle target eventually gets built against production infrastructure (not a laptop), this same pipeline logic would move to the managed `DataflowRunner`, at which point it absolutely would show up as a Dataflow job with real worker VMs, same as the BigQuery leg, because at that point there'd be a real network path (VPC peering, Cloud Interconnect, or similar) between Google Cloud and wherever Oracle lives.



⬅️ Back to [Guide Index](../README.md) | ➡️ Next: [Setting up the Postgres Pipeline](./10-setting-up-the-postgres-pipeline.md)
