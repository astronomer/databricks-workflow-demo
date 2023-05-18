"An example DAG that uses the DatabricksWorkflowTaskGroup to create a Databricks Workflow"
from pendulum import datetime

from airflow.decorators import dag, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from astro_databricks import DatabricksNotebookOperator, DatabricksWorkflowTaskGroup
from cluster_spec import job_cluster_spec


@dag(start_date=datetime(2023, 1, 1), schedule_interval="@daily", catchup=False)
def workflow_1():
    # the task group is a context manager that will create a Databricks Workflow
    with DatabricksWorkflowTaskGroup(
        group_id="example_databricks_workflow",
        databricks_conn_id="databricks_default",
        job_clusters=job_cluster_spec,
        # you can specify common fields here that get shared to all notebooks
        notebook_packages=[
            {"pypi": {"package": "simplejson"}},
        ],
        # notebook_params supports templating
        notebook_params={
            "start_time": "{{ ds }}",
        }
    ) as workflow:
        notebook_1 = DatabricksNotebookOperator(
            task_id="notebook_1",
            databricks_conn_id="databricks_default",
            notebook_path="/Shared/Notebook_1",
            source="WORKSPACE",
            # job_cluster_key corresponds to the job_cluster_key in the job_cluster_spec
            job_cluster_key="astro_databricks",
            # you can add to packages & params at the task level
            notebook_packages=[
                {"pypi": {"package": "faker"}},
            ],
            notebook_params={
                "end_time": "{{ macros.ds_add(ds, 1) }}",
            }
        )

        # you can embed task groups for easier dependency management
        @task_group(group_id="inner_task_group")
        def inner_task_group():
            notebook_2 = DatabricksNotebookOperator(
                task_id="notebook_2",
                databricks_conn_id="databricks_default",
                notebook_path="/Shared/Notebook_2",
                source="WORKSPACE",
                job_cluster_key="astro_databricks",
            )

            notebook_3 = DatabricksNotebookOperator(
                task_id="notebook_3",
                databricks_conn_id="databricks_default",
                notebook_path="/Shared/Notebook_3",
                source="WORKSPACE",
                job_cluster_key="astro_databricks",
            )

        notebook_4 = DatabricksNotebookOperator(
            task_id="notebook_4",
            databricks_conn_id="databricks_default",
            notebook_path="/Shared/Notebook_4",
            source="WORKSPACE",
            job_cluster_key="astro_databricks",
        )

        notebook_1 >> inner_task_group() >> notebook_4

    trigger_workflow_2 = TriggerDagRunOperator(
        task_id="trigger_workflow_2",
        trigger_dag_id="workflow_2",
        execution_date="{{ next_execution_date }}",
    )

    workflow >> trigger_workflow_2


workflow_1_dag = workflow_1()
