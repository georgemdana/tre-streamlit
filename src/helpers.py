from snowflake.snowpark.session import Session
import yaml
import streamlit as st
import pandas as pd

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

    def read_data(environment_name):
        with open(f"environments/{environment_name}", "r") as file:
            # Load the YAML data
            yaml_data = yaml.safe_load(file)
        db_name = yaml_data['db_name']
        fr_name = yaml_data['fr_name']
        wh_name = yaml_data['wh_name']
        return db_name, fr_name, wh_name
    
    def delete_environment(environment_name):
        db_name, fr_name, wh_name = helpers.read_data(environment_name)
        return ["USE ROLE SECURITYADMIN", f"DROP ROLE IF EXISTS {fr_name}", "USE ROLE SYSADMIN", f"DROP DATABASE IF EXISTS {db_name}", f"DROP WAREHOUSE IF EXISTS {wh_name}"]
    
    def query_executions(session, queries):
        object_status = []
        for i in queries:
            try:
                helpers.execute_sql(session, i)
                object_status.append("Query Succeeded")
            except Exception as e:
                object_status.append("Query Failed")
        queries_df = pd.DataFrame({
            'Query': queries,
            'Status': object_status
        })
        return queries_df




