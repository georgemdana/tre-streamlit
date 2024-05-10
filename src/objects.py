from helpers import helpers
class objects:
    def db_standup_query_gen(research_group_name, research_project):
        environment_name = f"{research_group_name.upper()}_{research_project.upper()}_TRE"
        return environment_name, f"CREATE DATABASE IF NOT EXISTS {environment_name}"

    def schema_standup_query_gen(schema, environment_name):
        schema_query = f"CREATE SCHEMA IF NOT EXISTS {environment_name}.{schema}"
        return schema_query
    
    def table_standup_query_gen(table, environment_name, schema, source_db, table_columns):
        columns = helpers.querify_list(table_columns)

        target_table = table.split(".", 2)
        table_query = f"CREATE TABLE IF NOT EXISTS {environment_name}.{schema}.{target_table[2]} AS SELECT {columns} FROM {table}"
        return table_query
    
    def functional_role_standup_query_gen(fr_name):
        functional_role_query = f"CREATE ROLE IF NOT EXISTS {fr_name.upper()}"
        return functional_role_query
    
    def warehouse_standup_query_gen(wh_name, wh_size):
        warehouse_query = f"CREATE WAREHOUSE IF NOT EXISTS {wh_name} WITH WAREHOUSE_SIZE='{wh_size}'"
        return warehouse_query