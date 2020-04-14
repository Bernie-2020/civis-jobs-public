# Container: 
# CONFIG_PATH: '/app/civis-jobs/civis_jobs/tools/matching/student-matching-config.R'

# Parameters --------------------------------------------------------------
matches_per_id = 3 # integer, number of matches allowed per source ID (will be deduplicated in output table)
enable_cass = TRUE # boolean, run CASS address standardization
rematch_threshold = .6 # decimal, rematch all records less than this match score on each update (automatically includes new records without scores in input table)
cutoff_threshold = .4 # decimal, keep all matches greater than or equal to this match score in final table
use_extra_match = TRUE # boolean, to include matches below threshold where states, first names, and last names tie out

# Source table and schema
# Can be an partial or complete source table (records already in destination table and above match threshold will be excluded from matching)
input_table_param = list(schema = 'student_data',
                         table = 'students_for_matching_staging')
                         
# Source table columns
pii_param = list(primary_key='student_hash',
                 first_name='first_name',
                 middle_name=NULL,
                 last_name='last_name',
                 phone='phone_1',
                 email='email',
                 full_address='address',
                 unit=NULL,
                 city='city',
                 state_code='state',
                 zip='zip',
                 gender=NULL,
                 birth_date=NULL,
                 birth_year=NULL,
                 birth_month=NULL,
                 birth_day=NULL,
                 house_number=NULL,
                 street=NULL,
                 lat=NULL,
                 lon=NULL)
                 
# Destination table and schema
# If this table already exists it will be unioned and deduplicated into the updated output table
output_table_param = list(schema = 'matching',
                          table = 'student_matched')
