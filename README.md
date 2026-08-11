# Dataflow Pipeline — A Beginner's Guide

A simple, easy-to-follow walkthrough on building Dataflow pipelines, starting with a batch load from `GCS` into `Cloud Spanner`, then moving into two streaming Change Data Capture pipelines from `Cloud Spanner`: one into `BigQuery` using a Google-provided template, and one into `Postgres` using a hand-written pipeline (a stand-in for the real Oracle target).

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

**Part 3 — Streaming: Spanner Change Streams to Postgres (Oracle stand-in)**

9. [Introduction: Why a Custom Pipeline, and Why No Dataflow Job Appears](./doc/09-postgres-pipeline-introduction.md)
10. [Setting up the Postgres Pipeline](./doc/10-setting-up-the-postgres-pipeline.md)
11. [Running and Verifying the Pipeline](./doc/11-running-and-verifying-the-postgres-pipeline.md)
12. [Troubleshooting the Postgres Pipeline](./doc/12-troubleshooting-postgres-pipeline.md)
