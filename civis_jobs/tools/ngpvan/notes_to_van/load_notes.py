from datetime import datetime
from jinja2 import Template
import logging
import civis
import os

from civis_jobs.tools.ngpvan.van_config import VAN_CIVIS_API_KEYS
from civis_jobs.state_syncs.projects import STATE_SYNC_PROJECTS
from civis_jobs.settings import Settings
import civis_jobs.utils as cu

# https://platform.civisanalytics.com/spa/#/scripts/containers/49031944

'''

In order to load data to newly created survey questions, a SQL file with a view
definition needs and the proper columns needs to be added to the views folder.
Once there, this job will attempt to load it.

'''

SET = Settings()

CLIENT = civis.APIClient()

STATE = os.environ['STATE']

# at some point might make sense to load to MyCampaign as well
base_name = f'{STATE} - Notes to VAN'


def patch_for_note_load(state_van_api_key, db, name, client=CLIENT):
    """Patches a Civis Export to VAN job to have the correct settings for
    loading notes."""

    if db == "myv":
        ngpvan_mode = "0"
    if db == "myc":
        ngpvan_mode = "1"

    args = {
        "CLUSTER": {
            "database": SET.civis_db_id,
            "credential": cu.get_db_cred_id()
            },
        # Not sure this is the right export type, just using the name given in civis
        "EXPORT_TYPE": "people_notes",
        "QUEUE_TABLE": f"van_note_loads.{STATE}_notes_current_load",
        "RESPONSE_TABLE": f"van_note_loads.{STATE}_notes_current_load_r",
        # drops the response table instead of appending
        # civis changes the columns sometimes
        "EXISTING_TABLE_ROWS": "drop",
        "NGPVAN_MODE": ngpvan_mode,
        "NGPVAN": state_van_api_key,
        "MODE": "run",
        "LOG_LEVEL": "DEBUG"
    }

    job_id = cu.get_job_id(name)

    client.scripts.patch_custom(
        id=job_id,
        arguments=args
    )

    logging.info(f'Patched job {job_id} {name}')



def main():

    from data import NOTE_CATEGORY_ID

    logging.basicConfig(level=logging.INFO)
    dir_path = os.path.dirname(os.path.realpath(__file__))

    now = datetime.now()
    datetime_string = now.strftime("%Y%m%d%H%M%s")

    # Note: run for myc only at this time.
    for db in ["myc"]:

        note_views = []

        # looping through the file names in the views directory
        for sql_file in os.listdir(f"{dir_path}/views/{db}"):
            if sql_file.endswith(".sql"):
                note_views.append(sql_file)

        # creates a job with the specified name if it does not exist already

        if db == "myc":
            name = base_name + ' (MyCampaign Import)'
        else:
            name = base_name + ' (MyVoters Import)'

        cu.create_civis_to_van_job(name=name)

        # getting the job id from the name
        job_id = cu.get_job_id(name)

        # reading in SQL need to create the new records table and handle the log
        data_prep_file = open(
            dir_path + f'/data_prep.sql', 'r')

        data_prep_sql = data_prep_file.read()
        data_prep_file.close()

        post_load_file = open(
            dir_path + f'/post_load.sql', 'r')

        post_load_sql = post_load_file.read()
        post_load_file.close()

        # looping through the view definitions for each note type
        for note in note_views:

            # create the view of all responses for the state
            sql_file = open(
                dir_path + f'/views/{db}/{note}', 'r')

            state_view_sql = sql_file.read()
            sql_file.close()

            sql = Template(state_view_sql).render(
                {'STATE': STATE,
                'NOTE_ID': NOTE_CATEGORY_ID[note[:-4]][STATE],
                # uses a utility function to change time zone from UTC to local
                'TIMEZONE_SQL': cu.timsezone_casewhen(
                    state_column='donor_state',
                    input_column='contacttimestamp',
                    output_column='date_canvassed')})

            logging.info(f'Creating view from {note} for {STATE} in {db}')

            civis.io.query_civis(sql=sql, database=SET.civis_db_name).result()

            # create new table with data to load
            sql = Template(data_prep_sql).render(
                {'STATE': STATE,
                 'DB': db,
                 'DATETIME_STR': datetime_string,
                 'NOTE': note[:-4]
                }
            )

            logging.info(f'Creating table of new {note[:-4]} records for {STATE} in {db}')
            civis.io.query_civis(sql=sql, database=SET.civis_db_name).result()

            # make sure the job has the correct settings
            patch_for_note_load(state_van_api_key=VAN_CIVIS_API_KEYS[STATE], db=db, name=name)

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
            logging.info(f'Finished loading {note[:-4]} records for {STATE}')

            # update log
            sql = Template(post_load_sql).render(
                {'STATE': STATE,
                 'DATETIME_STR': datetime_string,
                 'NOTE': note[:-4],
                 'DB': db
                },
            )

            civis.io.query_civis(sql=sql, database=SET.civis_db_name).result()
            logging.info(f'Updated log to reflect loaded records')


if __name__ == "__main__":
    main()
