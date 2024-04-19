from snowflake.snowpark.session import Session

class helpers:
    def create_snowpark_session(username, password, account, role = "ACCOUNTADMIN", warehouse = "COMPUTE_WH"):

        connection_params = {
        "user" : username,
        "password" : password,
        "account" : account,
        "role" : role,
        "warehouse" : warehouse
        }
        # create snowpark session
        session = Session.builder.configs(connection_params).create()
        return session

    def execute_sql(session, command):
        sql_command = session.sql(command)
        return sql_command.collect()



