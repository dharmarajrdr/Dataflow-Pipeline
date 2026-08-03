# 1. Introduction to Dataflow

- New to Dataflow? Here's the simple version: it is a Google Cloud service that lets you process data, either as a stream (live, continuous data) or as a batch (data all at once). It is built on top of Apache Beam.

- So why do people use it? Think of Dataflow as a helper that moves and changes large amounts of data for you, without you having to manage any servers or infrastructure yourself.

- Picture this: you have 10TB of data, and you need to read it, change its format, and save it somewhere else. Your laptop can't handle that. Doing it by hand would take forever and be full of mistakes.

- This is exactly where Dataflow steps in. It does the heavy lifting for you.
- **It automatically scales up or down, runs things in parallel, and keeps working even if something breaks along the way.**

- The only real job you have is to describe your pipeline logic (what to read, how to change it, where to write it). Dataflow takes care of everything else, no matter how big the dataset is.

- Who actually uses this in the real world? Big companies like PayPal use Dataflow for their data needs. In fact, PayPal uses it as part of their Cloud Modernization project to move data back from the cloud to their on-premise systems (this is called reverse replication). Since their data lives in Google Spanner, Dataflow reads it from there and pushes it to the next system in line.



### What we'll build in this guide

- To keep things simple, we'll use a sample file called [bulk records](../data/bulk-records.csv), which has 1,000 fake user records.
- This file will sit inside a [GCS bucket](https://console.cloud.google.com/storage/browser) (Google's cloud storage).
- The goal: get Dataflow to read this file, tweak the data a little, and save it into a Spanner table.



⬅️ Back to [Guide Index](../README.md) | ➡️ Next: [Creating a Pipeline](./02-creating-a-pipeline.md)
