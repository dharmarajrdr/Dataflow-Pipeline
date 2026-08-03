# 2. How Do You Create a Dataflow Pipeline?

- You've got two options here.

    |  | Method | What it means |
    |------|--------|-------------|
    | 1 | **Dataflow Template** | A ready-made template that Google already built for you. You can also build your own custom ones later. |
    | 2 | **Dataflow Job Builder** (newer option) | A drag-and-drop visual tool where you build your pipeline by clicking and connecting pieces, no template needed. |

- Go to the [Google Cloud Dataflow](https://console.cloud.google.com/dataflow/jobs) page to begin. Both options will show up there.

    ![](../.assets/Create%20Job.png)

---

### Let's Get Started

- Click on [Create job from template](https://console.cloud.google.com/dataflow/createjob).

- First time using this? Google might ask you to turn on the Dataflow API for your project. Just click `Enable`.

    ![](../.assets/Enable%20API.png)

- Next, you'll see a bunch of templates Google has already made. Since we want to move a file from GCS into Spanner, pick the `Text files on Cloud storage to Cloud Spanner` option from the dropdown list.

    ![](../.assets/Template%20Creation%20Form.png)

- Once you pick that template, it will ask you for two things: where the data is coming from (the `source`) and where it should go (the `target`).


---

⬅️ Back: [Introduction](./01-introduction.md) | ➡️ Next: [Setting up Source and Target](./03-setting-up-source-and-target.md)
