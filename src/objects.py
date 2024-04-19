from helpers import helpers

class objects:
    def db_standup_query_gen(session, research_group_name, research_project):
        helpers.execute_sql(session, "USE ROLE SYSADMIN") # swap to sysadmin for privileges
        environment_name = f"{research_group_name}_{research_project}_RG_ENVIRONMENT"
        return environment_name, f"CREATE DATABASE IF NOT EXISTS {environment_name}"
        # helpers.execute_sql(session, f"CREATE DATABASE IF NOT EXISTS {environment_name}") # stand up environment

    def schema_standup_query_gen(session, schemas, environment_name):
        schema_queries = []
        for schema in schemas:
            schema_query = f"CREATE SCHEMA IF NOT EXISTS {environment_name}.{schema}"
            schema_queries.append(schema_query)

