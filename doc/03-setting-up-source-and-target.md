# 3. Setting Up Your Source and Target

- We already picked our template. Now let's set up where the data comes from and where it should end up.



### Setting Up the Target — Cloud Spanner

- Go to [Cloud Spanner](https://console.cloud.google.com/spanner/instances).
    - Set up a new instance and a database.
    - Already have one? No need to make another, just use it.
    - Use [Spanner Studio](https://console.cloud.google.com/spanner/instances/dataflow-instance/databases/dataflow-db/details/query) to run your SQL queries directly.
    - Go ahead and create a table called `users` using this schema:

        ```sql
        CREATE TABLE users (
            id INT64 NOT NULL,
            name STRING(MAX) NOT NULL,
            email STRING(255) NOT NULL,
            phone STRING(50),
            country STRING(100),
            dob DATE
        ) PRIMARY KEY (id);
        ```

- That wraps up the target setup. On to the source.



### Setting Up the Source — GCS Bucket

- [Make a bucket](https://console.cloud.google.com/storage/create-bucket) with any unique name you like, and upload the [bulk-records.csv](../data/bulk-records.csv) file into it.

- Both source and target are ready now. Time to plug those details into the Dataflow template form.

    ![](../.assets/Source%20and%20Target.png)

- With source and target both set, there's one more question: how does Dataflow actually carry the data across? It needs some compute power to read from GCS, transform the data, and write it into Spanner. We'll cover that part next.



⬅️ Back: [Creating a Pipeline](./02-creating-a-pipeline.md) | ➡️ Next: [Running and Troubleshooting](./04-running-and-troubleshooting.md)
