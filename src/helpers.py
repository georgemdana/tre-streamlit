from snowflake.snowpark import Session
import yaml
import streamlit as st
import pandas as pd
from streamlit_dynamic_filters import DynamicFilters

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
    
    def save_env(environment_name, fr_name, wh_name, users, date, archive_status):
        data = {
            'db_name': environment_name,
            'fr_name': fr_name,
            'wh_name': wh_name,
            'users': users,
            'date_created': date,
            'archived': archive_status,
            'archive_date': "N/A"
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
        users = yaml_data['users']
        date_created = yaml_data['date_created']
        archive_status = yaml_data['archived']
        archive_date = yaml_data['archive_date']
        return db_name, fr_name, wh_name, users, date_created, archive_status, archive_date
    
    def delete_environment(environment_name):
        db_name, fr_name, wh_name, users, date_created, archive_status, archive_date = helpers.read_data(environment_name)
        return ["USE ROLE SECURITYADMIN", f"DROP ROLE IF EXISTS {fr_name}", "USE ROLE SYSADMIN", f"DROP DATABASE IF EXISTS {db_name}", f"DROP WAREHOUSE IF EXISTS {wh_name}"]

    def archive_environment(environment_name):
        db_name, fr_name, wh_name, users, date_created, archive_status, archive_date = helpers.read_data(environment_name)
        archive_query = ["USE ROLE SECURITYADMIN"]
        for user in users:
            archive_query += [f"REVOKE ROLE {fr_name} FROM USER {user}"]
        return archive_query

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

    def grant_query_executions(session, queries):
        grant_status = []
        for i in queries:
            try:
                helpers.execute_sql(session, i)
                grant_status.append("Query Succeeded")
            except Exception as e:
                grant_status.append("Query Failed")
        grant_queries_df = pd.DataFrame({
            'Query': queries,
            'Status': grant_status
        })
        return grant_queries_df
    
    def strip_yaml(dataframe):
        chars = '.yaml'
        stripped = ""
        stripped = [sub.replace(chars, '') for sub in dataframe]
        return stripped 

    def display_df_and_return(df, columns):
        dynamic_filters = DynamicFilters(df, filters=columns)
        dynamic_filters.display_filters(location='columns', num_columns=2, gap='small')
        dynamic_filters.display_df()
        return df

    def create_new_sf_table(session, table_name, columns, database, schema): #where columns is a dictionary including data types
        # Create the table
        role_query = ["USE ROLE ACCOUNTADMIN"]
        database_query = [f"USE DATABASE {database}"]
        schema_query = [f"USE SCHEMA {schema}"]
        create_table_sql = f"CREATE TABLE {table_name} ("
        for column, data_type in columns.items():
            create_table_sql += f"{column} {data_type}, "
        create_table_sql = create_table_sql[:-2] + ");"
        create_new_table_sql = role_query.append(create_table_sql)

        helpers.execute_sql(session, create_new_table_sql)
        
        comment = f"New Cohort Table Created in {table_name}."
        return comment

    def create_table_in_snowflake(session, table_name, database, schema_name, cols):
        try:
            session.cursor().execute(f"CREATE TABLE {database}.{schema_name}.{table_name} ({', '.join([f'{col} VARCHAR' for col in cols])});")
            st.write(f"Table {database}.{schema_name}.{table_name} created successfully!")
        except Exception as e:
            st.write(f"Error creating table in Snowflake: {e}")

    # Create a function to load a Pandas DataFrame into Snowflake
    def load_df_to_snowflake(session, df, table_name, schema_name):
        try:
            df.to_sql(
                name=table_name,
                schema=schema_name,
                con=session,
                if_exists='replace',
                index=False
            )
            st.write(f"Data loaded into {schema_name}.{table_name} successfully!")
        except Exception as e:
            st.write(f"Error loading data into Snowflake: {e}")
