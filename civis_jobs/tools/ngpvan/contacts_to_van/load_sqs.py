from datetime import datetime
from jinja2 import Template
import logging
import civis
import os

from civis_jobs.tools.ngpvan.van_config import VAN_CIVIS_API_KEYS
from civis_jobs.settings import Settings
import civis_jobs.utils as cu

# https://platform.civisanalytics.com/spa/#/scripts/containers/47257029

'''

In order to load data to newly created survey questions, a SQL file with a view
definition needs and the proper columns needs to be added to the views folder.
Once there, this job will attempt to load it.

'''

SET = Settings()

CLIENT = civis.APIClient()

STATE = os.environ['STATE']
# STATE = 'IL'

NAME = f'{STATE} - Contacts to VAN SQs'


def patch_for_sq_load(state_van_api_key, name=NAME, client=CLIENT, ngpvan_mode="0"):
    """Patches a Civis Export to VAN job to have the correct settings for
    loading survey questions."""

    assert ngpvan_mode in ["0", "1"], f"Invalid value for ngpvan_mode: {ngpvan_mode}"

    if ngpvan_mode == "0":
        name = name + ' (MyVoters Import)'
    if ngpvan_mode == "1":
        name = name + ' (MyCampaign Import)'

    # creates a job with the specified name if it does not exist already
    cu.create_civis_to_van_job(name)

    # getting the job id from the name
    job_id = cu.get_job_id(name)

    args = {
        "CLUSTER": {
            "database": SET.civis_db_id,
            "credential": cu.get_db_cred_id()},
        "EXPORT_TYPE": "surveys",
        "QUEUE_TABLE": f"van_sq_loads.{STATE}_sqs_current_load",
        "RESPONSE_TABLE": f"van_sq_loads.{STATE}_sqs_current_load_r",
        # drops the response table instead of appending
        # civis changes the columns sometimes
        "EXISTING_TABLE_ROWS": "drop",
        "NGPVAN_MODE": ngpvan_mode,
        "NGPVAN": state_van_api_key,
        "MODE": "run",
        "LOG_LEVEL": "DEBUG"
    }

    client.scripts.patch_custom(
        id=job_id,
        arguments=args
    )

    logging.info(f'Patched job {job_id} {name}')

    return job_id, name


def main():

    logging.basicConfig(level=logging.INFO)
    dir_path = os.path.dirname(os.path.realpath(__file__))

    now = datetime.now()
    datetime_string = now.strftime("%Y%m%d%H%M%s")

    sq_views = []

    # looping through the file names in the views directory

    for sql_file in os.listdir(dir_path + '/views'):
        if sql_file.endswith(".sql"):
            sq_views.append(sql_file)

    # reading in SQL need to create the new records table and handle the log
    data_prep_file = open(
        dir_path + f'/data_prep.sql', 'r')

    data_prep_sql = data_prep_file.read()
    data_prep_file.close()

    post_load_file = open(
        dir_path + f'/post_load.sql', 'r')

    post_load_sql = post_load_file.read()
    post_load_file.close()

    # looping through the view definitions for each survey question
    for sq in sq_views:

        # create the view of all responses for the state
        sql_file = open(
            dir_path + f'/views/{sq}', 'r')

        state_view_sql = sql_file.read()
        sql_file.close()

        sql = Template(state_view_sql).render(
            {'STATE': STATE,
             # uses a utility function to change time zone from UTC to local
             'TIMEZONE_SQL': cu.timsezone_casewhen(
                 state_column='state_code',
                 input_column='contacttimestamp',
                 output_column='date_canvassed')})

        logging.info(f'Creating view from {sq} for {STATE}')

        civis.io.query_civis(sql=sql, database=SET.civis_db_name).result()

        # create new table with data to load
        sql = Template(data_prep_sql).render(
            {'STATE': STATE,
             'DATETIME_STR': datetime_string,
             'SQ': sq[:-4]}
        )

        logging.info(f'Creating table of new {sq[:-4]} records for {STATE}')
        civis.io.query_civis(sql=sql, database=SET.civis_db_name).result()

        # make sure the job has the correct settings
        if sq.startswith("myc"):
            job_id, name = patch_for_sq_load(
                state_van_api_key=VAN_CIVIS_API_KEYS[STATE],
                ngpvan_mode = "1")

        # default is for data to go to MyVoters
        else:
            job_id, name = patch_for_sq_load(
                state_van_api_key=VAN_CIVIS_API_KEYS[STATE])

        # run the job
        run = CLIENT.scripts.post_custom_runs(job_id)
        logging.info(f'Running {name} {job_id}')

        # uses a future object to ensure this script fails if the child fails
        poller = CLIENT.scripts.get_custom_runs
        poller_args = job_id, run.id
        polling_interval = 10

        job_future = civis.futures.CivisFuture(
            poller, poller_args, polling_interval)

        job_future.result()
        logging.info(f'Finished loading {sq[:-4]} records for {STATE}')

        # update log
        sql = Template(post_load_sql).render(
            {'STATE': STATE,
             'DATETIME_STR': datetime_string,
             'SQ': sq[:-4]}
        )

        civis.io.query_civis(sql=sql, database=SET.civis_db_name).result()
        logging.info(f'Updated log to reflect loaded records')


if __name__ == "__main__":
    main()
