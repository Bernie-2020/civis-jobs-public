from datetime import datetime
import pandas as pd
import logging
import civis
from civis.base import CivisAPIError
import ast

from civis_jobs.settings import Settings

# container: add link to audit container here

CLIENT = civis.APIClient()
SET = Settings()

# Export table names, update the tablenames for your db
workflow_table = 'civis_jobs_meta.workflows'
job_table = 'civis_jobs_meta.workflow_jobs'

# list ids of workflows that are unscheduled but need to be monitored
WORKFLOWS = [] 

def create_workflows_data(client):
    # Civis returns a generator
    workflow_gen = client.workflows.list(iterator=True)
    all_workflow_data = list()

    for workflow_meta in workflow_gen:
        # handling for nested schedule information
        schedule_meta = workflow_meta['schedule']
        del workflow_meta['schedule']
        workflow_meta = {**workflow_meta, **schedule_meta}

        # taking only the user's name from nested user information
        workflow_meta['user'] = workflow_meta['user']['name']

        # grabbing the metadata for each workflow to get te job ids
        single_workflow_meta = client.workflows.get(
            id=workflow_meta['id'])

        # getting job ids from the workflow's configuration file
        job_id_list = single_workflow_meta['definition'].split(
            sep='job_id: ')

        # removing the first element of the list which has no job id
        job_id_list = job_id_list[1:]

        # new line split, keeps only the first element which is are the ids
        job_ids = [job_id.split(sep='\n')[0] for job_id in job_id_list]

        # adding the job ids to the data
        workflow_meta['job_ids'] = job_ids

        all_workflow_data.append(workflow_meta)

    # remove jobs that are not scheduled
    df = pd.DataFrame(all_workflow_data)
    df = df[(df['scheduled'] == True) | (df['id'].isin(WORKFLOWS))]

    return df


def latest_runtime(job_meta):
    # parses the output of client.jobs.get() and returns the runtime for the
    # most recent execution of the job
    if job_meta['runs']:
        # if the jobs is running, None is returned
        if job_meta['runs'][0]['state'] != 'running':

            try:
                # takes only the hour, minute, seconds for the start and end times
                # of a jobs most recent run
                latest_start_time = job_meta['runs'][0]['started_at'][11:19]
                latest_end_time = job_meta['runs'][0]['finished_at'][11:19]

                # changes the strings to datetime objects for subtraction
                latest_start_time = datetime.strptime(
                    latest_start_time, "%H:%M:%S")

                latest_end_time = datetime.strptime(
                    latest_end_time, "%H:%M:%S")

                runtime = latest_end_time - latest_start_time
                # turns datetime delta back to a string
                return str(runtime)
            except:
                return None

        else:
            return None

    else:
        return None


def create_workflow_jobs_data(client, workflows_schematable):

    jobs_query = civis.io.read_civis_sql(
        sql=f'''select name as workflow_name,
            job_ids from {workflows_schematable}''',
        database=SET.civis_db_name)[1:]

    data = []

    # looping through the jobs in each scheduled workflow
    for job in jobs_query:

        job_ids = job[1]
        # turns the string version of the list into a real list
        job_ids = ast.literal_eval(job_ids)
        logging.debug(job_ids)

        # hack removal of periodic bad values
        job_ids = [int(x) for x in job_ids if x not in (None, 'None')]
        logging.debug(job_ids)

        for job_id in job_ids:
            logging.debug(job_id)

            if job_id <= 0:
                continue

            try:
                job_meta = client.jobs.get(job_id)
                # calculating the runtime of the most recent run
                job_meta['latest_runtime'] = latest_runtime(job_meta)

                job_meta['workflow'] = job[0]
                del job_meta['runs']
                del job_meta['schedule']

                data.append(job_meta)
                
            except CivisAPIError as e:
                logging.info((job_id))

    return pd.DataFrame(data)


def main():
    logging.basicConfig(level=logging.INFO)

    workflows_data = create_workflows_data(CLIENT)
    logging.info("Created data for workflows")

    civis.io.dataframe_to_civis(
        df=workflows_data,
        database=SET.civis_db_name,
        table=workflow_table,
        existing_table_rows='drop',
        client=CLIENT).result()

    workflow_jobs_data = create_workflow_jobs_data(
        CLIENT, workflow_table)

    logging.info("Created data for scheduled workflow jobs")

    civis.io.dataframe_to_civis(
        df=workflow_jobs_data,
        database=SET.civis_db_name,
        table=job_table,
        existing_table_rows='drop',
        client=CLIENT).result()


if __name__ == '__main__':
    main()
