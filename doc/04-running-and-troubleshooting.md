# 4. Running the Pipeline & Fixing Common Issues

- Hit the `Run` button. It'll take a bit of time because Dataflow needs to spin up a worker node to actually process your data. A worker node is basically just a [VM instance](https://console.cloud.google.com/compute/instances) working behind the scenes. Keep an eye on the `Current workers` number in the Job Info section.



### Pipeline Failing? Check Your IAM Permissions First

- Run this command to see what permissions are currently set:

    ```shell
    gcloud projects get-iam-policy dharma-learn-gcp --filter="bindings.members:474421665985-compute@developer.gserviceaccount.com"
    ```

- If you see `Listed 0 items.`, that means your service account is missing the `dataflow.worker` role. Add it by running this in Cloud Shell:

    ```shell
    gcloud projects add-iam-policy-binding dharma-learn-gcp \
    --member="serviceAccount:474421665985-compute@developer.gserviceaccount.com" \
    --role="roles/dataflow.worker"
    ```

- You might also run into an error that looks like this:

    ```
    Startup of the worker pool in us-central1 failed to bring up any of the desired 2 workers. This is likely a quota issue or a Compute Engine stockout. The service will retry. For troubleshooting steps, see https://cloud.google.com/dataflow/docs/guides/common-errors#worker-pool-failure for help troubleshooting. ZONE_RESOURCE_POOL_EXHAUSTED: Instance 'df-bulkusers-job-08030804-i7mu-harness-6179' creation failed: The zone 'projects/dharma-learn-gcp/zones/us-central1-a' does not have enough resources available to fulfill the request.  Try a different zone, or try again later.
    ```

- This usually means that particular zone has run out of free capacity. It'll keep retrying in the same zone on its own, but don't just sit and wait. **Stop** the pipeline, switch to a different zone in the `Job Info` section, then run it again.

- One thing to remember: once you've fixed the permission issue, you can't just re-run the old failed pipeline. You'll need to clone it and run the clone instead.

    ![](../.assets/Job%20Info.png)

- When things are working, you'll see a message like `Starting a pool of 2 workers.`. Head over to [Compute Engine](https://console.cloud.google.com/compute/instances) and you'll spot 2 new VM instances, these are your worker nodes doing the actual data processing.

    ![](../.assets/VM%20Init.png)

- These VMs handle reading from GCS, transforming the data, and writing it into Spanner. Once the job wraps up, Dataflow automatically deletes these VMs for you, no manual cleanup needed.



### When the Job Finishes

- Once everything's done, the Job Info section will show `Job state: Done`.

- And that's it, you've just built and run a Dataflow pipeline using Google's default template!



⬅️ Back: [Setting up Source and Target](./03-setting-up-source-and-target.md) | ➡️ Next: [Introduction to Change Streams](./05-change-streams-introduction.md)
