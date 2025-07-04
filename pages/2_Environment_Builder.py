import streamlit as st
import snowflake.connector
from snowflake.snowpark import Session

import sys
sys.path.append('./src')
from helpers import helpers
from objects import objects
from policies import policies
import pandas as pd
import os
from datetime import date
import yaml

# secrets management
from dotenv import load_dotenv

load_dotenv("/Users/danageorge/Documents/Hakkoda_Github/Trusted-Research-Environment/environments/.env")

username = os.getenv("username")
password = os.getenv("password")
account = os.getenv("account")
role = os.getenv("role")
warehouse = os.getenv("warehouse")

## App Start
st.image('frostbanner.png', width = 300)
#st.image('frostlogo.png', width = 300)
st.caption("Trusted Research Environment Set Up Tool")

# use this function to generate all queries needed for object stand-up and return them as a list
def object_query_generation():
    ## this function cycles through all schemas under our chosen database and generates queries for db, schema, table and FR creation
    object_queries = ["USE ROLE SYSADMIN"]
    environment_name, envrionment_query = objects.db_standup_query_gen(research_group_name, research_project) #generate database create statement
    object_queries.append(envrionment_query)

    warehouse_query = objects.warehouse_standup_query_gen(wh_name, wh_size)
    object_queries.append(warehouse_query)

    for schema in source_schemas:
        schema_query = objects.schema_standup_query_gen(schema, environment_name)
        object_queries.append(schema_query)
        # tables = globals()[f"{schema}_tables"]

        tables = table_dictionary[f"{source_db}.{schema}"]
        object_queries.append("USE ROLE DATA_ADMIN")
        for table in tables:
            table_query = objects.table_standup_query_gen(table, environment_name, schema, source_db, column_dictionary[f"{table}"])
            object_queries.append(table_query)

    object_queries.append("USE ROLE SECURITYADMIN")
    functional_role_query = objects.functional_role_standup_query_gen(fr_name) #generate functional role creation query
    object_queries.append(functional_role_query)
    return object_queries

# # use this function to generate all queries needed for object stand-up and return them as a list
# def object_query_generation():
#     ## this function cycles through all schemas under our chosen database and generates queries for db, schema, table and FR creation
#     object_queries = ["USE ROLE SYSADMIN"]
#     object_query_table = ["USE ROLE DATA_ADMIN"]
#     environment_name, envrionment_query = objects.db_standup_query_gen(research_group_name, research_project) #generate database create statement
#     object_queries.append(envrionment_query)

#     warehouse_query = objects.warehouse_standup_query_gen(wh_name, wh_size)
#     object_queries.append(warehouse_query)

#     for schema in source_schemas:
#         schema_query = objects.schema_standup_query_gen(schema, environment_name)
#         object_queries.append(schema_query)
#         tables = table_dictionary[f"{source_db}.{schema}"]
#         # tables = globals()[f"{schema}_tables"]
#         # for table in tables:
#         #     table_query = objects.table_standup_query_gen(table, environment_name, schema, source_db, column_dictionary[f"{table}"])
#         #     object_query_table.append(table_query)

#     for i in tables:
#             columns = pd.DataFrame(helpers.execute_sql(session, f"SHOW COLUMNS IN TABLE {i}"))["column_name"].tolist()
#             source_columns = st.multiselect(label = f"Source Columns From Table: {i}", options = columns)
#             column_dictionary[f"{i}"] = source_columns
    
#     for table in tables:
#         column_dictionary = {}
#         table_query = objects.table_standup_query_gen(table, environment_name, schema, source_db, column_dictionary = {f"{table}"})
#         object_queries.append(schema_query)
#         tables = table_dictionary[f"{source_db}.{schema}"]

#     object_queries.append("USE ROLE SECURITYADMIN")
#     functional_role_query = objects.functional_role_standup_query_gen(fr_name) #generate functional role creation query
#     object_queries.append(functional_role_query)
#     return object_queries

# use this function to generate all queries needed for grant stand-up and return them as a list
def privilege_query_generation():
    environment_name, environment_query = objects.db_standup_query_gen(research_group_name, research_project)
    statements = policies.fr_privilege_statement_gen(session, fr_name, privileges, environment_name)
    statements_2 = policies.fr_assignment_statement_gen(statements, fr_name, users)
    privilege_statements = policies.wh_assignment_statement_gen(statements_2, fr_name, wh_name)
    return privilege_statements

session = helpers.create_snowpark_session(username, password, account, role, warehouse)
environment_creation, object_standup, grant_setup = st.tabs(["Environment Creation", "Object Creation", "Grant Set Up"])

if 'environment_creator' not in st.session_state:
    st.session_state.environment_creator = {}
if 'object_status' not in st.session_state:
    st.session_state.object_status = {}
if 'grant_status' not in st.session_state:
    st.session_state.grant_status = {}

with environment_creation:
    helpers.execute_sql(session, "USE ROLE SYSADMIN")
    research_group_name = st.text_input(label = "Research Group Name") # No spaces
    research_project = st.text_input(label = "Research Project Name") # No spaces
    fr_name = f"FR_TRE_{research_group_name.upper()}_{research_project.upper()}"
    wh_name = f"WH_TRE_{research_group_name.upper()}_{research_project.upper()}"
    environment_name = f"TRE_{research_group_name.upper()}_{research_project.upper()}"
    
    # fetch desired warehouse size
    wh_sizes = ["XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE", "XXXLARGE", "X4LARGE", "X5LARGE", "X6LARGE"]
    wh_size = st.selectbox(label = "Warehouse Size", options = wh_sizes)

    # fetch privileges needed for users
    privileges_options = ["SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES", "ALTER", "USAGE", "OWNERSHIP"]
    privileges = st.multiselect(label = "Table Privileges", options = privileges_options)
    #st.session_state.privileges = st.multiselect(label = "Table Privileges", options = privileges_options)

    # fetch users needed for research environment
    users_options = pd.DataFrame(helpers.execute_sql(session, f"SHOW USERS"))["name"].tolist()
    users = st.multiselect(label = "Users", options = users_options) # To be given access
    
    dbs = pd.DataFrame(helpers.execute_sql(session, f"SHOW DATABASES"))["name"].tolist()
    source_db = st.selectbox(label = "Source Database", options = dbs) # Select a source data base for this environment
    table_dictionary = {}
    
    # schema selection wizard
    if len(source_db) > 0:
        schemas = pd.DataFrame(helpers.execute_sql(session, f"SHOW SCHEMAS IN DATABASE {source_db}"))["name"].tolist()
        source_schemas = st.multiselect(label = "Source Schemas", options = schemas)

    # table selection wizard
    if len(source_schemas) > 0:
        table_dictionary = {}
        for i in source_schemas:
            tables = pd.DataFrame(helpers.execute_sql(session, f"SHOW TABLES IN SCHEMA {source_db}.{i}"))["name"].tolist()
            tables = [f"{source_db}.{i}." + s for s in tables]
            source_tables = st.multiselect(label = f"Source Tables From Schema: {source_db}.{i}", options= tables)
            table_dictionary[f"{source_db}.{i}"] = source_tables
        
    # column selection wizard
    if len(table_dictionary) > 0:
        #st.markdown("""---""")
        #st.write("Select columns here")
        #st.markdown("""---""")
        column_dictionary = {}
        tables = []
        for i in source_schemas:
            schema = f"{source_db}.{i}"
            tables.extend(table_dictionary[schema])
        print(tables)
        for i in tables:
            columns = pd.DataFrame(helpers.execute_sql(session, f"SHOW COLUMNS IN TABLE {i}"))["column_name"].tolist()
            source_columns = st.multiselect(label = f"Source Columns From Table: {i}", options = columns)
            column_dictionary[f"{i}"] = source_columns
        print(column_dictionary)

    # query generation and execution wizard
    if len(research_group_name) > 0 and len(research_project) > 0 and len(wh_size) > 0 and len(users) > 0 and len(source_db) > 0 and len(source_db) > 0 and len(source_schemas) > 0 and len(table_dictionary) > 0 and len(column_dictionary) > 0:
        st.write("    ")
        st.write("**Wizard is successfully filled out. Please continue to the :blue[Object Creation tab.]**")
        environment_creator = "ran"
        st.session_state.environment_creator = environment_creator

with object_standup:
    if len(research_group_name) > 0 and len(research_project) > 0 and len(wh_size) > 0 and len(users) > 0 and len(source_db) > 0 and len(source_db) > 0 and len(source_schemas) > 0 and len(table_dictionary) > 0 and len(column_dictionary) > 0 and st.session_state.environment_creator == "ran":
        object_queries = object_query_generation()
        st.subheader("Research Environment Object Query Standup and Execution")

        st.write("**Please review the following queries. Ensure they align with the environment you would like to stand up.**")
        for query in object_queries:
            st.write(query)
        if st.button("Run Snowflake Object Creation Scripts"):
            object_queries_df = helpers.query_executions(session = session, queries = object_queries)
            helpers.save_env(environment_name, fr_name, wh_name, users, date.today(), "False")
            st.table(object_queries_df)
            run_variable = "exists"
            st.write("**Please use the second column :blue['Status'] to confirm all queries successfully ran before continuing.**")
            object_status = "ran"
            st.session_state.object_status = object_status
    else:
        st.write("**Please fill out the :blue[Environment Creation] tab before moving to this page.**")


with grant_setup:
    if st.session_state.object_status == "ran":
        privilege_queries = privilege_query_generation()
        st.subheader("Research Environment Grant Query Standup and Execution")

        st.write("**Please review the following queries and ensure they align with the grants you need for your environment.**")
        for query in privilege_queries:
            st.write(query)
        if st.button("Run Snowflake Grant Scripts"):
            grants_queries_df = helpers.grant_query_executions(session = session, queries = privilege_queries)
            st.table(grants_queries_df)
            grant_run_variable = "exists"
            st.write("**Please use the second column :blue['Status'] to confirm all queries successfully ran before continuing.**")
            grant_status = "ran"
            st.session_state.grant_status = grant_status
            

    else:
        st.write("**Please fill out the :blue[Object Creation tab] before moving to Grant Set Up**")


    #     try:
    #         st.write("**Please review the following queries and ensure they align with the grants you need for your environment.**")
    #         #privilege_queries = []
    #         privilege_queries = privilege_query_generation()
    #     except:
    #         st.write("**No queries found, please ensure you've run all object standup queries before moving to this page.**")
    #         #privilege_queries = []
    #     for query in privilege_queries:
    #         st.write(query)
    #     if st.button("Run Snowflake Grants Scripts"):
    #         #privilege_queries = []
    #         privilege_queries = privilege_query_generation()
    #         grants_queries_df = helpers.query_executions(session = session, queries = privilege_queries)
    #         st.table(grants_queries_df)
    #         st.session_state.button2_clicked = True
    # else:
    #     st.write("**Please fill out the :blue[Object Creation tab] before moving to Grant Set Up**")

# with grant_setup:
#     if len(research_group_name) > 0 and len(research_project) > 0 and len(wh_size) > 0 and len(users) > 0 and len(source_db) > 0 and len(source_db) > 0 and len(source_schemas) > 0 and len(table_dictionary) > 0 and len(column_dictionary) > 0 and object_status == "ran":
#         st.subheader("Research Environment Grant Query Standup and Execution")
#         st.write("**Please review the following queries and ensure they align with the grants you need for your environment.**")
#         privilege_queries = privilege_query_generation()
    
#         for query in privilege_queries:
#             st.write(query)

#         if st.button("Run Snowflake Grants Scripts"):
#             privilege_queries = privilege_query_generation()
#             grants_queries_df = helpers.query_executions(session = session, queries = privilege_queries)
#             st.table(grants_queries_df)
#             st.session_state.button2_clicked = True
#     else:
#         st.write("**Please fill out the :blue[Object Creation tab] before moving to Grant Set Up**")


# with grant_setup:
#     if len(research_group_name) > 0 and len(research_project) > 0 and len(wh_size) > 0 and len(users) > 0 and len(source_db) > 0 and len(source_db) > 0 and len(source_schemas) > 0 and len(table_dictionary) > 0 and len(column_dictionary) > 0 and object_status == "ran":
#         st.subheader("Research Environment Grant Query Standup and Execution")
#         try:
#             st.write("**Please review the following queries and ensure they align with the grants you need for your environment.**")
#             privilege_queries = privilege_query_generation()
#         except:
#             st.write("**No queries found, please ensure you've run all object standup queries before moving to this page.**")
#             privilege_queries = []
#         for query in privilege_queries:
#             st.write(query)
#         if st.button("Run Snowflake Grants Scripts"):
#             privilege_queries = privilege_query_generation()
#             grants_queries_df = helpers.query_executions(session = session, queries = privilege_queries)
#             st.table(grants_queries_df)
#             st.session_state.button2_clicked = True
#     else:
#         st.write("**Please fill out the :blue[Object Creation tab] before moving to Grant Set Up**")

# with environment_management:
#     environments = [ f for f in os.listdir("environments/") if f.endswith('.yaml') ]
#     if len(environments) == 0:
#         st.write("**No Environments Detected. Please Create an Environment to Get Started**")
#     else:
#         for environment in environments:
#             db_name, fr_name, wh_name, users, date_created, archive_status, archive_date = helpers.read_data(environment)
#             col1, col2 = st.columns(2)
#             with col1:
#                 st.write(f"Environment Name: {environment.split('.')[0]}")
#                 st.write(f"Functional Role: {fr_name}")
#                 st.write(f"Warehouse: {wh_name}")
#                 st.write(f"Users: {users}")
#                 st.write(f"Date Created: {date_created}")
#                 st.write(f"Archive Status: {archive_status}")
#                 st.write(f"Archive Date: {archive_date}")
#                 st.markdown("""---""")

#             with col2:
#                 if st.button("Delete Environment", key = environment):
#                     delete_statements = helpers.delete_environment(environment)
#                     for statement in delete_statements:
#                         helpers.execute_sql(session, statement)
#                     os.remove(f"environments/{environment}")
#                     st.experimental_rerun()
#                 if archive_status == 'False':
#                     if st.button("Archive Environment", key = f"{environment}_archive"):
#                         archive_statements = helpers.archive_environment(environment)
#                         for statement in archive_statements:
#                             helpers.execute_sql(session, statement)
#                         with open(f"environments/{environment}", "r") as file:
#                             data = yaml.safe_load(file)
                        
#                         data['archived'] = 'True'
#                         data['archive_date'] = date.today()

#                         with open(f"environments/{environment}", "w") as file:
#                             yaml.dump(data, file)
#                         st.experimental_rerun()