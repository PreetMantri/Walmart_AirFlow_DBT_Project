import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

ws = WorkspaceClient(
    host='TEST',
    token = 'TEST'
)

job_trigger = ws.jobs.run_now(job_id=  TEST)

while True:

    job_run = ws.jobs.get_run(job_trigger.run_id)

    print(f"Job run state: {job_run.state.life_cycle_state}, result state: {job_run.state.result_state}")
    if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
        if job_run.state.result_state == RunResultState.SUCCESS:
            print("Job completed successfully.")
            break
        else:
            raise Exception(f"Job failed with result state: {job_run.state.result_state}")
        
    time.sleep(5)  # Wait for 5 seconds before checking the job status again
    