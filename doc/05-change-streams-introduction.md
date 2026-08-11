# 5. Introduction to Spanner Change Streams → BigQuery

- The first part of this guide covered a **batch** pipeline: read a file once, transform it, load it. That's great for one-off or scheduled loads, but it doesn't help if you need to know about changes as they happen.

- This second part covers a **streaming** pipeline instead: we'll watch a Spanner table live, and every time a row is inserted, updated, or deleted, that change gets pushed downstream automatically, within seconds.

- The Google Cloud feature that makes this possible is called **Change Streams**. Think of it as a live subscription to a Spanner table: instead of asking "what does this table look like right now?", you're asking "tell me about everything that happens to this table, forever, as it happens."



### Why does this matter for PayPal's Cloud Modernization work?

- As mentioned earlier, PayPal uses Dataflow as part of reverse replication, moving data from Spanner back to on-premise systems. Change Streams is the mechanism that makes that kind of continuous, near-real-time sync possible, instead of running expensive full-table batch jobs over and over.



### What we'll build in this guide

- **Source**: a Spanner table called `Orders1`, with a Change Stream watching it.
- **Target 1 (this guide)**: BigQuery, using a Google-provided template, no custom code required.
- **Target 2 (future work)**: Oracle, the actual end goal for this modernization effort. This will need a custom JDBC-based sink, since Google doesn't ship a ready-made Spanner Change Streams → Oracle template. We'll tackle that separately once BigQuery is fully validated.

- The pipeline shape looks like this:

    ```
    Spanner (Orders1 table)
          │
          ▼
    Change Stream (cs_orders1)
          │
          ▼
    Dataflow (Spanner_Change_Streams_to_BigQuery template)
          │
          ▼
    BigQuery (Orders1_changelog table)
    ```

- One thing worth calling out before we start: this pipeline writes an **append-only changelog**, not a mirrored copy of the table. Every INSERT, UPDATE, and DELETE shows up as its own new row in BigQuery, rather than overwriting the previous state. We'll see exactly why, and how to work with it, once we get data flowing.



⬅️ Back to [Guide Index](../README.md) | ➡️ Next: [Setting up the Source and Target](./06-setting-up-the-cdc-pipeline.md)
