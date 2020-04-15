from datetime import datetime
from jinja2 import Template
import logging
import civis
import os
#from civis.utils import run_template

from civis_jobs.tools.ngpvan.van_config import VAN_CIVIS_API_KEYS
from civis_jobs.settings import Settings
import civis_jobs.utils as cu
from civis import APIClient
from civis.futures import CivisFuture

# workflow: https://platform.civisanalytics.com/spa/#/workflows/12205
# template job: https://platform.civisanalytics.com/spa/#/scripts/containers/56186973

'''

In order to load data to newly people, a SQL file with a view
definition needs and the proper columns needs to be added to the views folder.
Once there, this job will attempt to load it.

'''

SET = Settings()

CLIENT = civis.APIClient()

STATE = os.environ['STATE']

VAN_TEMPLATE_ID = 19204

def run_job(job_id, api_key=None, client=None, polling_interval=None):
    """Run a job.

    Parameters
    ----------
    job_id: str or int
        The ID of the job.
    api_key: DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client: :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    polling_interval : int or float, optional
        The number of seconds between API requests to check whether a result
        is ready.

    Returns
    -------
    results: :class:`~civis.futures.CivisFuture`
        A `CivisFuture` object.
    """
    if client is None:
        client = APIClient(api_key=api_key)
    run = client.jobs.post_runs(job_id)
    return CivisFuture(
        client.jobs.get_runs,
        (job_id, run["id"]),
        client=client,
        polling_interval=polling_interval,
        poll_on_creation=False,
    )



def run_template(id, arguments, JSONValue=False, client=None):
    """Run a template and return the results.

    Parameters
    ----------
    id: int
        The template id to be run.
    arguments: dict
        Dictionary of arguments to be passed to the template.
    JSONValue: bool, optional
        If True, will return the JSON output of the template.
        If False, will return the file ids associated with the
        output results.
    client: :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.

    Returns
    -------
    output: dict
        If JSONValue = False, dictionary of file ids with the keys
        being their output names.
        If JSONValue = True, JSON dict containing the results of the
        template run. Expects only a single JSON result. Will return
        nothing if either there is no JSON result or there is more
        than 1 JSON result.


    """
    if client is None:
        client = APIClient()
    job = client.scripts.post_custom(id, arguments=arguments)
    run = client.scripts.post_custom_runs(job.id)
    fut = CivisFuture(
        client.scripts.get_custom_runs, (job.id, run.id), client=client
    )
    fut.result()
    outputs = client.scripts.list_custom_runs_outputs(job.id, run.id)
    if JSONValue:
        json_output = [
            o.value for o in outputs if o.object_type == "JSONValue"
        ]
        if len(json_output) == 0:
            log.warning("No JSON output for template {}".format(id))
            return
        if len(json_output) > 1:
            log.warning(
                "More than 1 JSON output for template {}"
                " -- returning only the first one.".format(id)
            )
        # Note that the cast to a dict is to convert
        # an expected Response object.
        return dict(json_output[0])
    else:
        file_ids = {o.name: o.object_id for o in outputs}
        return file_ids

def main():

    logging.basicConfig(level=logging.INFO)
    dir_path = os.path.dirname(os.path.realpath(__file__))

    now = datetime.now()
    datetime_string = now.strftime("%Y%m%d%H%M%s")

    sql_views = []

    # looping through the file names in the views directory
    for sql_file in os.listdir(dir_path + '/views'):
        if sql_file.endswith(".sql"):
            sql_views.append(sql_file)


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
    for ppl in sql_views:
        # get shortened name from view filename
        PPL = ppl[:-4]
        # create the view of all responses for the state
        sql_file = open(
            dir_path + f'/views/{ppl}', 'r')

        state_view_sql = sql_file.read()
        sql_file.close()

        sql = Template(state_view_sql).render(
            {'STATE': STATE,
             'PPL': PPL,
             })

        logging.info(f'Creating view from {ppl} for {STATE}')
        civis.io.query_civis(sql=sql, database=SET.civis_db_name).result()


        # create new table with data to load
        sql = Template(data_prep_sql).render(
            {'STATE': STATE,
             'DATETIME_STR': datetime_string,
             'PPL': PPL}
        )

        logging.info(f'Creating table of new {PPL} records for {STATE}')
        civis.io.query_civis(sql=sql, database=SET.civis_db_name).result()

        # run template people loader job
        logging.info(f'Running template {VAN_TEMPLATE_ID} for {STATE}')
        run_template(id = VAN_TEMPLATE_ID,
                                 arguments = {
                                        "CLUSTER": {
                                            "database": SET.civis_db_id,
                                            "credential": cu.get_db_cred_id()},
                                        "EXPORT_TYPE": "people",
                                        "QUEUE_TABLE": f"van_people_loads.{STATE}_people_current_load",
                                        "RESPONSE_TABLE": f"van_people_loads.{STATE}_people_current_load_r",
                                        "EXISTING_TABLE_ROWS": "drop",
                                        "NGPVAN_MODE": "1",
                                        "NGPVAN": VAN_CIVIS_API_KEYS[STATE],
                                        "MODE": "run",
                                        "LOG_LEVEL": "DEBUG"
    })

        # update log
        sql = Template(post_load_sql).render(
            {'STATE': STATE,
             'DATETIME_STR': datetime_string,
             'PPL': PPL}
        )

        civis.io.query_civis(sql=sql, database=SET.civis_db_name).result()
        logging.info(f'Updated log to reflect loaded records')


if __name__ == "__main__":
    main()
