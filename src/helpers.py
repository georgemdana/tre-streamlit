from snowflake.snowpark.session import Session
import yaml

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
    
    def execute_sql_pandas(session, command):
        sql_command = session.sql(command)
        return sql_command.to_pandas()
    
    def querify_list(list):
        string = ""
        for i in list:
            string = string + f"{i}, "
        string = string[:-2]
        return string
    
    def save_env(environment_name, fr_name, wh_name):
        data = {
            'db_name': environment_name,
            'fr_name': fr_name,
            'wh_name': wh_name
        }

        yaml_file_path = f'environments/{environment_name}.yaml'

        with open(yaml_file_path, 'w') as file:
            yaml.dump(data, file)


