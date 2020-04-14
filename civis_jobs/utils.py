from civis_jobs.settings import Settings

from datetime import datetime
from functools import wraps
from time import time
import civis
import hashlib
import json
import logging
import os
import inspect
import requests
import secrets
import string

SET = Settings()

CLIENT = civis.APIClient()

logging.basicConfig(level=logging.INFO)


def bool_conv(val):
    return val == 'true'


def comma_str_to_list(comma_str, cast_func=None):
    if not comma_str:
        return []

    if not cast_func:
        return comma_str.split(",")
    else:
        return [cast_func(x) for x in comma_str.split(",")]


def list_to_comma_str(lst):
    return ','.join([str(x) for x in lst])


def table_exists(table_name, table_schema, db=SET.civis_db_name):
    """Checks to see if a table or view already exists"""

    table_name = table_name.lower()
    table_schema = table_schema.lower()

    sql = ' '.join(['select true where EXISTS (',
                    'select * from information_schema.tables',
                    f"where table_schema = '{table_schema}'",
                    f"and table_name = '{table_name}');"])

    schema_table = table_schema + '.' + table_name

    try:
        civis.io.read_civis_sql(
            sql=sql,
            database=db,
            use_pandas=False)

        logging.info(f'{schema_table} exists')

        return True

    except Exception:

        logging.info(f'{schema_table} does not exist')
        return False


def get_job_id(name, client=CLIENT):
    """Gets a civis job id"""

    # civis function does a fuzzy match
    # jobs.list is a broken endpoint now
    r = client.search.list(query=name)

    # this loop returns only exact matches
    for j in r['results']:
        if j['name'] == name:
            return j['id']

    return None


def get_db_cred_id(client=CLIENT):
    """Get the id for a users Redshift credential in civis. Ideally this
    should be replaced with the get_database_credential_id endpoint."""
    username = client.users.list_me()['username']
    user_creds = client.credentials.list()

    # luckily for a user's redshift credential, name and username are the same
    for cred in user_creds:

        if (cred['username'] == username) & (cred['type'] == 'Database'):

            return cred['id']

    return None


def job_exists(name, client=CLIENT, iterator=False):
    """Checks to see if a civis job already exists"""
    job_id = get_job_id(name)

    if job_id:
        return True

    if not job_id:
        return False


def create_civis_to_van_job(name, client=CLIENT):
    """Creates a civis to van job if one does not arleady exist"""

    bool_status = job_exists(name=name)

    if not bool_status:

        script = CLIENT.scripts.post_custom(
            # template ID for Export Civis Data to VAN
            from_template_id=19204,
            name=name
        )

        logging.info(f'Created job {name}')

        return script["id"]

    if bool_status:
        logging.info(f'Job {name} exists')

        return get_job_id(name, client=client)


def create_job_from_template(name, template_id, client=CLIENT):
    """Creates a civis to van job if one does not arleady exist"""

    bool_status = job_exists(name=name)

    if not bool_status:

        script = CLIENT.scripts.post_custom(
            from_template_id=template_id,
            name=name
        )

        logging.info(f'Created job {name}')

        return script["id"]

    if bool_status:
        logging.info(f'Job {name} exists')

        return get_job_id(name, client=client)


def time_between_in_est(intv_start, intv_end):
    """Checks to see if the current time is between a start time and end time
    in EST with military formatting minus the colon. This is used to keep jobs
    in workflows that run more than once a day without the specific job
    running more than needed."""

    # TODO: assert that intv_start > intv_end

    # subtract 4 hours to go from UTC to EST
    current_est = (int(str(
        datetime.utcnow().time()).replace(':', '')[0:4]) - 400)

    if current_est >= intv_start and current_est <= intv_end:
        return True

    else:
        return False


def set_up_environment_for_parsons(redshift="REDSHIFT", aws="AWS"):

    # S3 API Keys
    os.environ['AWS_ACCESS_KEY_ID'] = os.getenv(f"{aws}_ACCESS_KEY_ID", "")
    os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv(
        f"{aws}_SECRET_ACCESS_KEY", "")

    # Redshift
    os.environ['REDSHIFT_USERNAME'] = os.getenv(
        f"{redshift}_CREDENTIAL_USERNAME", "")
    os.environ['REDSHIFT_PASSWORD'] = os.getenv(
        f"{redshift}_CREDENTIAL_PASSWORD", "")
    os.environ['REDSHIFT_HOST'] = os.getenv(f"{redshift}_HOST", "")
    os.environ['REDSHIFT_DB'] = os.getenv(f"{redshift}_DATABASE", "")
    os.environ['REDSHIFT_PORT'] = os.getenv(f"{redshift}_PORT", "")

    # For copying data into Redshift from S3
    os.environ['S3_TEMP_BUCKET'] = "temp-data-load"


def md5_hash(string):
    return hashlib.md5(string.encode("utf-8")).hexdigest()


def upload_file_as_civis_script_outputs(filename, civis_job_id=None,
                                        civis_run_id=None):
    """Upload a file as Output to a Civis Script.

    Currently only supports container scripts. Note: The scripts must be
    running when this function is called.

    `Args:`
        civis_job_id: int
            The job id for a Civis container script.
        civis_run_id: int
            The run id for a Civis container script run.
    """
    job_id = civis_job_id if civis_job_id else os.getenv("CIVIS_JOB_ID")
    run_id = civis_run_id if civis_run_id else os.getenv("CIVIS_RUN_ID")

    if job_id and run_id:
        with open(filename, "r") as f:
            file_id = civis.io.file_to_civis(f, filename)

        client = civis.APIClient()
        client.scripts.post_containers_runs_outputs(
            job_id, run_id, 'File', file_id)


def upload_logs_as_civis_script_outputs(lgr):
    """Upload the logs files as Outputs to a Civis Script.

    If the script is running locally, then the function does nothing.

    `Args:`
        lgr: logging.Logger
            The logger object with file handlers.
    """
    for handle in lgr.handlers:
        if isinstance(handle, logging.FileHandler):
            log_file = handle.baseFilename

            upload_file_as_civis_script_outputs(log_file)


def post_json_outputs(data, output_name="", job_id=None, run_id=None):
    if not job_id and not run_id:
        job_id = os.getenv("CIVIS_JOB_ID")
        run_id = os.getenv("CIVIS_RUN_ID")

    # Check again to see if there were in the environment
    if not job_id and not run_id:
        return

    if isinstance(data, dict):
        json_outputs = [
            {"value_str": json.dumps(val), "name": key}
            for key, val in data.items()
        ]

    else:
        json_outputs = [
            {"value_str": json.dumps(data), "name": output_name}
        ]

    for output in json_outputs:
        # save output to civis
        json_value_object = CLIENT.json_values.post(**output)

        # post it as a run output
        CLIENT.scripts.post_containers_runs_outputs(
            job_id, run_id, 'JSONValue', json_value_object["id"])


def timeit(func):
    """Modify a function to return result and runt statistics.

    Example:
    .. code-block:: python
        >>> from utils import timeit

        >>> @timeit
        ... def my_function(arg1):
        ...    return f"The are is {arg1}"
        ...
        >>> my_function("hello")
        ("The are is hello", {"function": "my_function", "args": ("hello"),
        "kwargs": (), "runtime": 0.0001})


    `Args:`
        func: callable
            The function to wrap.
    `Returns:`
        tuple
            The first item in the tuple is the result of the wrapped function,
            or None, the second item is a dicitonary with run statistics.

    """
    @wraps(func)
    def wrap(*args, **kwargs):
        try:
            ts = time()
            result = func(*args, **kwargs)
        except Exception:
            te = time()
            print(f"Function failed after {te-ts:2.4f} sec.")
            print(
                f'function:{func.__name__} args:[{args}, {kwargs}] '
                f'took: {te-ts:2.4f} sec')
            post_json_outputs({
                "function": func.__name__,
                "args": args,
                "kwargs": kwargs,
                "runtime": f"{te-ts}",
            }, "timeit")
            raise

        te = time()
        # if do_print:
        #     print(
        #         f'function:{f.__name__} args:[{args}, {kwargs}] '
        #         f'took: {te-ts:2.4f} sec')

        result = (result, {
            "function": func.__name__,
            "args": args,
            "kwargs": kwargs,
            "runtime": f"{te-ts}",
        })

        return result
    return wrap


def seconds_to_text(secs, as_time_str=False):
    # Adapted from
    # https://gist.github.com/Highstaker/280a09591df4a5fb1363b0bbaf858f0d
    days = secs//86400
    hours = (secs - days*86400)//3600
    minutes = (secs - days*86400 - hours*3600)//60
    seconds = secs - days*86400 - hours*3600 - minutes*60

    days_text = "day{}".format("s" if days != 1 else "")
    hours_text = "hour{}".format("s" if hours != 1 else "")
    minutes_text = "minute{}".format("s" if minutes != 1 else "")
    seconds_text = "second{}".format("s" if seconds != 1 else "")

    if as_time_str:
        result = ":".join(filter(lambda x: bool(x), [
            "{0} {1}".format(int(days), days_text) if days else "",
            "{0:02d}".format(int(hours)) if hours else "",
            "{0:02d}".format(int(minutes)) if minutes else "00",
            "{0:02.4f}".format(seconds) if seconds else "00"
        ]))
    else:
        result = ", ".join(filter(lambda x: bool(x), [
            "{0} {1}".format(int(days), days_text) if days else "",
            "{0} {1}".format(int(hours), hours_text) if hours else "",
            "{0} {1}".format(int(minutes), minutes_text) if minutes else "",
            "{0:.4f} {1}".format(seconds, seconds_text) if seconds else ""
        ]))
    return result


def timsezone_casewhen(state_column, input_column, output_column):
    '''
    Creates a case when used to change a datetime in UTC to rough local time.
    The conversion is not perfect, we made the decision to settle for the
    timezone that covers the majority of the state. Organizing said this was
    sufficient, it was mostly important to get the day correct as much as
    possible.

        Params
            state_column (str) : The name of the column with state values. Can be
                either the abbreviation or the name of the state spelled out.

            input_column (str) : The name of the datetime column to be converted.

            output_column (str) : The name for the resulting column.

        Returns
            (str) : A string with column names piped in. Commas are not
                included on either side of the string.
    '''

    return f"""CASE
            WHEN upper({state_column}) = 'AL'
                or upper({state_column}) = 'ALABAMA'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'AK'
                or upper({state_column}) = 'ALASKA'
                then convert_timezone('US/Alaska', {input_column}::timestamp)

            WHEN upper({state_column}) = 'AZ'
                or upper({state_column}) = 'ARIZONA'
                then convert_timezone('US/Mountain', {input_column}::timestamp)

            WHEN upper({state_column}) = 'AR'
                or upper({state_column}) = 'ARKANSAS'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'CA'
                or upper({state_column}) = 'CALIFORNIA'
                then convert_timezone('US/Pacific', {input_column}::timestamp)

            WHEN upper({state_column}) = 'CO'
                or upper({state_column}) = 'COLORADO'
                then convert_timezone('US/Mountain', {input_column}::timestamp)

            WHEN upper({state_column}) = 'CT'
                or upper({state_column}) = 'CONNECTICUT'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'DE'
                or upper({state_column}) = 'DELAWARE'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'FL'
                or upper({state_column}) = 'FLORIDA'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'GA'
                or upper({state_column}) = 'GEORGIA'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'HI'
                or upper({state_column}) = 'HAWAII'
                then convert_timezone('US/Hawaii', {input_column}::timestamp)

            WHEN upper({state_column}) = 'ID'
                or upper({state_column}) = 'IDAHO'
                then convert_timezone('US/Mountain', {input_column}::timestamp)

            WHEN upper({state_column}) = 'IL'
                or upper({state_column}) = 'ILLINOIS'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'IN'
                or upper({state_column}) = 'INDIANA'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'IA'
                or upper({state_column}) = 'IOWA'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'KS'
                or upper({state_column}) = 'KANSAS'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'KY'
                or upper({state_column}) = 'KENTUCKY'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'LA'
                or upper({state_column}) = 'LOUISIANA'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'ME'
                or upper({state_column}) = 'MAINE'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'MD'
                or upper({state_column}) = 'MARYLAND'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'MA'
                or upper({state_column}) = 'MASSACHUSETTS'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'MI'
                or upper({state_column}) = 'MICHIGAN'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'MN'
                or upper({state_column}) = 'MINNESOTA'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'MS'
                or upper({state_column}) = 'MISSISSIPPI'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'MO'
                or upper({state_column}) = 'MISSOURI'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'MT'
                or upper({state_column}) = 'MONTANA'
                then convert_timezone('US/Mountain', {input_column}::timestamp)

            WHEN upper({state_column}) = 'NE'
                or upper({state_column}) = 'NEBRASKA'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'NV'
                or upper({state_column}) = 'NEVADA'
                then convert_timezone('US/Pacific', {input_column}::timestamp)

            WHEN upper({state_column}) = 'NH'
                or upper({state_column}) = 'NEW HAMPSHIRE'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'NJ'
                or upper({state_column}) = 'NEW JERSEY'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'NM'
                or upper({state_column}) = 'NEW MEXICO'
                then convert_timezone('US/Mountain', {input_column}::timestamp)

            WHEN upper({state_column}) = 'NY'
                or upper({state_column}) = 'NEW YORK'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'NC'
                or upper({state_column}) = 'NORTH CAROLINA'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'ND'
                or upper({state_column}) = 'NORTH DAKOTA'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'OH'
                or upper({state_column}) = 'OHIO'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'OK'
                or upper({state_column}) = 'OKLAHOMA'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'OR'
                or upper({state_column}) = 'OREGON'
                then convert_timezone('US/Pacific', {input_column}::timestamp)

            WHEN upper({state_column}) = 'PA'
                or upper({state_column}) = 'PENNSYLVANIA'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'RI'
                or upper({state_column}) = 'RHODE ISLAND'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'SC'
                or upper({state_column}) = 'SOUTH CAROLINA'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'SD'
                or upper({state_column}) = 'SOUTH DAKOTA'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'TN'
                or upper({state_column}) = 'TENNESSEE'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'TX'
                or upper({state_column}) = 'TEXAS'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'UT'
                or upper({state_column}) = 'UTAH'
                then convert_timezone('US/Mountain', {input_column}::timestamp)

            WHEN upper({state_column}) = 'VT'
                or upper({state_column}) = 'VERMONT'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'VA'
                or upper({state_column}) = 'VIRGINIA'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'WA'
                or upper({state_column}) = 'WASHINGTON'
                then convert_timezone('US/Pacific', {input_column}::timestamp)

            WHEN upper({state_column}) = 'DC'
                or upper({state_column}) = 'WASHINGTON DC'
                or upper({state_column}) = 'D.C.'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'WV'
                or upper({state_column}) = 'WEST VIRGINIA'
                then convert_timezone('US/Eastern', {input_column}::timestamp)

            WHEN upper({state_column}) = 'WI'
                or upper({state_column}) = 'WISCONSIN'
                then convert_timezone('US/Central', {input_column}::timestamp)

            WHEN upper({state_column}) = 'WY'
                or upper({state_column}) = 'WYOMING'
                then convert_timezone('US/Mountain', {input_column}::timestamp)
                else NULL end as {output_column}"""


def wait_for_script(script_type, job_id, run_id, client=CLIENT,
                    polling_interval=10, return_future=False):
    """
    Wait for a script to finish.

    If the script fails, this function will raise an error. Alternateively,
    you can pass `return_future=True` and return the future.

    `Args:`
        script_type: str
            The type of Civis script it is. One of 'custom', 'python', 'r',
            'sql', 'container'.
        job_id: int
            The id of the script.
        run_id: int
            The id of the run.
        client: civis.APIClient
            (Optional) A civis client to use to run and wait for the script.
        polling_interval: int
            (Optional) The interval to wait before checking agian if the script
            has completed.
        return_future: bool
            (Optional) If `True` returns a future insead of the result of the
            of the script. Defaults to `False`.

        `Returns:`
            The result of the script.
    """
    script_types = {
        "custom": client.scripts.get_custom_runs,
        "python": client.scripts.get_python3_runs,
        "r": client.scripts.get_r_runs,
        "sql": client.scripts.get_sql_runs,
        "container": client.scripts.get_containers_runs,
        "workflow": client.workflows.get_executions,
        "imports": client.imports.get_files_runs,
    }

    script = client.search.list(query=job_id, type=script_type)

    name = script["results"][0]["name"]

    logging.info(
        f"Waiting for {name} (job: {job_id}, run: {run_id}) to finish...")

    # uses a future object to ensure this script fails if the child fails
    poller = script_types[script_type]
    poller_args = job_id, run_id

    job_future = civis.futures.CivisFuture(
        poller, poller_args, polling_interval)

    if return_future:
        return job_future

    return job_future.result()


def run_and_wait_for_script(script_type, job_id, client=CLIENT,
                            polling_interval=10, return_future=False):
    """
    Run a script and wait for it to finish.

    If the script fails, this function will raise an error. Alternateively,
    you can pass `return_future=True` and return the future.

    `Args:`
        script_type: str
            The type of Civis script it is. One of 'custom', 'python', 'r',
            'sql', 'container'.
        job_id: int
            The id of the script to run.
        client: civis.APIClient
            (Optional) A civis client to use to run and wait for the script.
        polling_interval: int
            (Optional) The interval to wait before checking agian if the script
            has completed.
        return_future: bool
            (Optional) If `True` returns a future insead of the result of the
            of the script. Defaults to `False`.

        `Returns:`
            The result of the script.
    """
    script_types = {
        "custom": client.scripts.post_custom_runs,
        "python": client.scripts.post_python3_runs,
        "r": client.scripts.post_r_runs,
        "sql": client.scripts.post_sql_runs,
        "container": client.scripts.post_containers_runs,
        "workflow": client.workflows.post_executions,
        "imports": client.imports.post_files_runs,
    }

    script = client.search.list(query=job_id, type=script_type)
    name = script["results"][0]["name"]
    logging.info(
        f"Creating a run for {name} (job: {job_id})...")

    run = script_types[script_type](job_id)

    return wait_for_script(
        script_type, job_id, run["id"],
        client=client,
        polling_interval=polling_interval,
        return_future=return_future
    )


def generate_password(length=12, req_lower=True, req_upper=True,
                      req_digit=True, req_char=True, valid_chars="!$%~#&+"):
    """Generate a basic password.

        `Args:`
            length: int
                The length of the password.
            req_lower: bool
                If ``True``, ensures the password contains a lowercase letter.
                Defaults to ``True``.
            req_upper: bool
                If ``True``, ensures the password contains an uppercase letter.
                Defaults to ``True``.
            req_digit: bool
                If ``True``, ensures the password contains a digit. Defaults
                to ``True``.
            req_char:
                If ``True``, ensures the password contains a non-word
                character. Defaults to ``True``.
            valid_chars: string
                A list of valid characters to include in the password.
    """
    alphabet = string.ascii_letters + string.digits + valid_chars

    tests = []

    if req_lower:
        tests.append(lambda pwd: any(c.islower() for c in pwd))

    if req_upper:
        tests.append(lambda pwd: any(c.isupper() for c in pwd))

    if req_digit:
        tests.append(lambda pwd: sum(c.isdigit() for c in pwd) >= 3)

    if req_char:
        tests.append(lambda pwd: any(c in valid_chars for c in pwd))

    while True:
        password = ''.join(secrets.choice(alphabet) for i in range(length))
        if (all(test(password) for test in tests)):
            break
    return password


def create_one_time_secret(secret, url_password=None, expires_in=600,
                           recipient=None, ots_username=None,
                           ots_password=None):
    base_url = "https://onetimesecret.com/api/v1"
    args = {
        "secret": secret,
        "passphrase": url_password,
        "ttl": expires_in,
        "recipient": recipient,
    }

    ots_username = ots_username or os.environ["OTS_USERNAME"]
    ots_password = ots_password or os.environ["OTS_PASSWORD"]

    resp = requests.post(
        f"{base_url}/share", params=args, auth=(ots_username, ots_password))
    data = resp.json()
    secret_url = (f"{base_url.replace('/api/v1', '')}/"
                  f"secret/{data['secret_key']}")

    return secret_url


def get_my_dirpath():
    """Return the /full/path/to/calling/script"""
    # based on https://stackoverflow.com/a/55469882
    # get the caller's stack frame and extract its file path
    frame_info = inspect.stack()[1]
    filepath = frame_info.filename
    # drop the reference to the stack frame to avoid reference cycles
    del frame_info

    # make the path absolute (optional)
    filepath = os.path.dirname(os.path.abspath(filepath))
    return filepath
