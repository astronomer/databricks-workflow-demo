"A simple workflow that uses Databricks Repos to run a notebook stored in git"
from pendulum import datetime

from airflow.decorators import dag
from astro_databricks import DatabricksNotebookOperator, DatabricksWorkflowTaskGroup
from cluster_spec import job_cluster_spec


@dag(start_date=datetime(2023, 1, 1), schedule_interval="@daily", catchup=False)
def workflow_git():
    with DatabricksWorkflowTaskGroup(
        group_id="git_example",
        databricks_conn_id="databricks_default",
        job_clusters=job_cluster_spec,
        extra_job_params={
            # this needs to be set up in the Databricks workspace
            # follow the instructions here: https://docs.databricks.com/en/repos/repos-setup.html
            # or if using a service principal: https://docs.databricks.com/en/repos/ci-cd-techniques-with-repos.html#use-sp-repos
            "git_source": {
                "git_url": "https://github.com/astronomer/astro-provider-databricks-notebooks",
                "git_provider": "gitHub",
                "git_branch": "main"
            }
        }
    ):
        notebook_1 = DatabricksNotebookOperator(
            task_id="notebook_1",
            databricks_conn_id="databricks_default",
            notebook_path="notebook_1", # don't need the .py extension
            source="GIT",
            job_cluster_key="astro_databricks",
        )

        notebook_1


workflow_git()
