import sys
sys.path.append('./src')
from helpers import helpers
from objects import objects

session = helpers.create_snowpark_session('tcaulton', 'Foo1234!', 'IZB40366', 'accountadmin', 'compute_wh')
print(session)

research_group_name = "HAKKODA_CANCER_RESEARCH"
research_project = "CURE"
db = "DEV"
schemas = ["LAKE_WORKDAY", "LAKE_REDCAP"]
lake_workday_tables = ["CR_EE_WORKDAY_RESCINDED_TRANSACTION", "CR_ENG_WORKDAY_RESCINDED_TRANSACTION"]
lake_redcap_tables = ["BH_FTE_FORM"]
CR_ENG_WORKDAY_RESCINDED_TRANSACTION_COLUMNS = ["STAFFING_EVENT_WID", "WORKER"]
CR_EE_WORKDAY_RESCINDED_TRANSACTION_COLUMNS = ["WORKDAY_ID", "TRANSACTION_STATUS"]


environment_name, envrionment_query = objects.db_standup_query_gen(session, research_group_name, research_project)
schema_queries = objects.schema_standup_query_gen(session, schemas, environment_name)