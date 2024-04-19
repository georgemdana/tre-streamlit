import sys
sys.path.append('./src')
from helpers import helpers
from objects import objects
from policies import policies

def object_query_generation():
    ## this function cycles through all schemas under our chosen database and generates queries for db, schema, table and FR creation
    environment_name, envrionment_query = objects.db_standup_query_gen(research_group_name, research_project) #generate database create statement
    object_queries.append(envrionment_query)

    warehouse_query = objects.warehouse_standup_query_gen(wh_name, wh_size)
    object_queries.append(warehouse_query)

    for schema in schemas:
        schema_query = objects.schema_standup_query_gen(schema, environment_name)
        object_queries.append(schema_query)
        tables = globals()[f"{schema}_tables"]
        for table in tables:
            table_query = objects.table_standup_query_gen(table, environment_name, schema, source_db, globals()[f"{table}_COLUMNS"])
            object_queries.append(table_query)
    object_queries.append("USE ROLE SECURITYADMIN")
    functional_role_query = objects.functional_role_standup_query_gen(fr_name) #generate functional role creation query
    object_queries.append(functional_role_query)
    return object_queries

def privilege_query_generation():
    environment_name, environment_query = objects.db_standup_query_gen(research_group_name, research_project)
    statements = policies.fr_privilege_statement_gen(session, fr_name, privileges_list, environment_name)
    statements_2 = policies.fr_assignment_statement_gen(statements, fr_name, users)
    privilege_statements = policies.wh_assignment_statement_gen(statements_2, fr_name, wh_name)
    return privilege_statements

session = helpers.create_snowpark_session('tcaulton', 'Foo1234!', 'IZB40366', 'accountadmin', 'compute_wh')
print(session)

object_queries = ["USE ROLE SYSADMIN"]
research_group_name = "HAKKODA_CANCER_RESEARCH"
research_project = "CURE"
source_db = "DEV"
schemas = ["LAKE_WORKDAY", "LAKE_REDCAP"]
LAKE_WORKDAY_tables = ["CR_EE_WORKDAY_RESCINDED_TRANSACTION", "CR_ENG_WORKDAY_RESCINDED_TRANSACTION"]
LAKE_REDCAP_tables = ["BH_FTE_FORM"]
CR_ENG_WORKDAY_RESCINDED_TRANSACTION_COLUMNS = ["STAFFING_EVENT_WID", "WORKER"]
CR_EE_WORKDAY_RESCINDED_TRANSACTION_COLUMNS = ["WORKDAY_ID", "TRANSACTION_STATUS"]
BH_FTE_FORM_COLUMNS = ["RECORD_ID", "SUBMISSION_REASON"]
fr_name = f"FR_{research_group_name.upper()}_{research_project.upper()}"
wh_name = f"WH_{research_group_name.upper()}_{research_project.upper()}"
wh_size = "XSMALL"
privileges_list = ["SELECT", "INSERT"]
users = ["TCAULTON"]

# environment_name, envrionment_query = objects.db_standup_query_gen(research_group_name, research_project)
# statements = policies.fr_privilege_statement_gen(session, fr_name, privileges_list, environment_name)
# statements_2 = policies.fr_assignment_statement_gen(statements, fr_name, users)
# statements_final = policies.wh_assignment_statement_gen(statements_2, fr_name, wh_name)
# print(statements_final)

object_queries = object_query_generation()
for i in object_queries:
    helpers.execute_sql(session, i)

privilege_queries = privilege_query_generation()
helpers.execute_sql(session, "USE ROLE SECURITYADMIN") # swap to sysadmin for privileges
for i in privilege_queries:
    helpers.execute_sql(session, i)