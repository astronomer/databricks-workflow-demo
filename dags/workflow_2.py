"A much simpler workflow that gets triggered after workflow_1"
from pendulum import datetime

from airflow.decorators import dag
from astro_databricks import DatabricksNotebookOperator, DatabricksWorkflowTaskGroup
from cluster_spec import job_cluster_spec


@dag(start_date=datetime(2023, 1, 1), schedule_interval="@daily", catchup=False)
def workflow_2():
    with DatabricksWorkflowTaskGroup(
        group_id="example_databricks_workflow",
        databricks_conn_id="databricks_default",
        job_clusters=job_cluster_spec,
        notebook_packages=[
            {"pypi": {"package": "simplejson"}},
        ],
    ):
        notebook_1 = DatabricksNotebookOperator(
            task_id="notebook_1",
            databricks_conn_id="databricks_default",
            notebook_path="/Shared/Notebook_1",
            source="WORKSPACE",
            job_cluster_key="astro_databricks",
            notebook_packages=[
                {"pypi": {"package": "faker"}},
            ],
        )

        notebook_2 = DatabricksNotebookOperator(
            task_id="notebook_2",
            databricks_conn_id="databricks_default",
            notebook_path="/Shared/Notebook_2",
            source="WORKSPACE",
            job_cluster_key="astro_databricks",
        )

        notebook_1 >> notebook_2


workflow_2_dag = workflow_2()
