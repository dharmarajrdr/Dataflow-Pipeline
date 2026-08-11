# Dataflow Pipeline — A Beginner's Guide

A simple, easy-to-follow walkthrough on building Dataflow pipelines, starting with a batch load from `GCS` into `Cloud Spanner`, then moving into a streaming Change Data Capture pipeline from `Cloud Spanner` into `BigQuery`.

### Table of Contents

**Part 1 — Batch: GCS to Cloud Spanner**

1. [Introduction to Dataflow](./doc/01-introduction.md)
2. [Creating a Pipeline](./doc/02-creating-a-pipeline.md)
3. [Setting up Source and Target](./doc/03-setting-up-source-and-target.md)
4. [Running and Troubleshooting](./doc/04-running-and-troubleshooting.md)

**Part 2 — Streaming: Spanner Change Streams to BigQuery**

5. [Introduction to Change Streams](./doc/05-change-streams-introduction.md)
6. [Setting up the Source, Target, and Permissions](./doc/06-setting-up-the-cdc-pipeline.md)
7. [Running and Verifying the Pipeline](./doc/07-running-and-verifying-the-pipeline.md)
8. [Troubleshooting the Change Streams Pipeline](./doc/08-troubleshooting-cdc-pipeline.md)

