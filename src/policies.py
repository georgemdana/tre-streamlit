from helpers import helpers
import pandas as pd

class policies:
    def fr_privilege_statement_gen(session, fr_name, privileges_list, environment_name):
        #privileges_statements = ["USE ROLE SECURITYADMIN", f"GRANT USAGE ON DATABASE {environment_name} TO ROLE {fr_name}"]
        privileges_statements = ["USE ROLE SECURITYADMIN"]
        grantdb_statement = f"GRANT USAGE ON DATABASE {environment_name} TO ROLE {fr_name}"
        privileges_statements.append(grantdb_statement)
        privileges = helpers.querify_list(privileges_list)
        for i in privileges_list:
            pr_assignment_query = f"GRANT USAGE ON DATABASE {environment_name} TO ROLE {fr_name}"
        schemas = pd.DataFrame(helpers.execute_sql(session, f"SHOW SCHEMAS IN DATABASE {environment_name}"))["name"].tolist()
        for schema in schemas:
            if schema != "INFORMATION_SCHEMA":
                privileges_statements.append(f"GRANT USAGE ON SCHEMA {environment_name}.{schema} TO ROLE {fr_name}")
            tables = pd.DataFrame(helpers.execute_sql(session, f"SHOW TABLES IN SCHEMA {environment_name}.{schema}"))#["name"].tolist()
            if tables.empty:
                continue
            else:
                tables = tables["name"].tolist()
            for table in tables:
                privilege_statement = f"GRANT {privileges} ON TABLE {environment_name}.{schema}.{table} TO ROLE {fr_name}"
                privileges_statements.append(privilege_statement)
        return privileges_statements

    def fr_assignment_statement_gen(privileges_statements, fr_name, users):
        for user in users:
            fr_assignment_query = f"GRANT ROLE {fr_name} TO USER {user}"
            privileges_statements.append(fr_assignment_query)
        return privileges_statements
    
    def wh_assignment_statement_gen(privileges_statements, fr_name, wh_name):
        wh_assignment_query = f"GRANT USAGE ON WAREHOUSE {wh_name} TO ROLE {fr_name}"
        privileges_statements.append(wh_assignment_query)
        return privileges_statements

    # def add_classic_role_priv(session, privilege_statements, environment_name):
    #     allow_datadmin_db = f"GRANT USAGE ON DATABASE {environment_name} TO ROLE DATA_ADMIN"
    #     allow_dataadmin_future_schemas = f"GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {environment_name} TO ROLE DATA_ADMIN"
    #     allow_dataadmin_future_tables = f"GRANT SELECT ON FUTURE TABLES IN DATABASE {environment_name} TO ROLE DATA_ADMIN"
    #     # privileges_statements.append = ["USE ROLE SECURITYADMIN", f"GRANT USAGE ON DATABASE {environment_name} TO ROLE DATA_ADMIN"]
    #     # privileges_statements.append = ["USE ROLE SECURITYADMIN", f"GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {environment_name} TO ROLE DATA_ADMIN"]
    #     # privileges_statements.append = ["USE ROLE SECURITYADMIN", f"GRANT SELECT ON FUTURE TABLES IN DATABASE {environment_name} TO ROLE DATA_ADMIN"]
    #     return privileges_statements