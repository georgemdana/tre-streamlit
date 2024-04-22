import streamlit as st
import sys
sys.path.append('./src')
from helpers import helpers
from objects import objects
from policies import policies
import pandas as pd

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
        tables = table_dictionary[f"{source_db}.{schema}"]
        # tables = globals()[f"{schema}_tables"]
        for table in tables:
            table_query = objects.table_standup_query_gen(table, environment_name, schema, source_db, column_dictionary[f"{table}"])
            object_queries.append(table_query)
    object_queries.append("USE ROLE SECURITYADMIN")
    functional_role_query = objects.functional_role_standup_query_gen(fr_name) #generate functional role creation query
    object_queries.append(functional_role_query)
    return object_queries

def privilege_query_generation():
    environment_name, environment_query = objects.db_standup_query_gen(research_group_name, research_project)
    statements = policies.fr_privilege_statement_gen(session, fr_name, privileges, environment_name)
    statements_2 = policies.fr_assignment_statement_gen(statements, fr_name, users)
    privilege_statements = policies.wh_assignment_statement_gen(statements_2, fr_name, wh_name)
    return privilege_statements

session = helpers.create_snowpark_session('tcaulton', 'Foo1234!', 'IZB40366', 'accountadmin', 'compute_wh')
environment_creation, environment_management = st.tabs(["Environment Creation", "Environment Management"])

with environment_creation:
    helpers.execute_sql(session, "USE ROLE SYSADMIN")
    research_group_name = st.text_input(label = "what is the name of your research group")
    research_project = st.text_input(label = "what is the name of this reserach project")
    fr_name = f"FR_{research_group_name.upper()}_{research_project.upper()}"
    wh_name = f"WH_{research_group_name.upper()}_{research_project.upper()}"
    environment_name = f"{research_group_name.upper()}_{research_project.upper()}_RG_ENVIRONMENT"
    

    wh_sizes = ["XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE", "XXXLARGE", "X4LARGE", "X5LARGE", "X6LARGE"]
    wh_size = st.selectbox(label = "Please select a warehouse size for researchers", options = wh_sizes)

    privileges_options = ["SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES", "ALTER", "USAGE", "OWNERSHIP"]
    privileges = st.multiselect(label = "Please select what table privileges you want researchers to have", options = privileges_options)

    users_options = pd.DataFrame(helpers.execute_sql(session, f"SHOW USERS"))["name"].tolist()
    users = st.multiselect(label = "What users should be granted access", options = users_options)
    
    dbs = pd.DataFrame(helpers.execute_sql(session, f"SHOW DATABASES"))["name"].tolist()
    source_db = st.selectbox(label = "please select a source database for this environment", options = dbs)
    table_dictionary = {}
    if len(source_db) > 0:
        st.markdown("""---""")
        st.write("select schemas here")
        st.markdown("""---""")
        schemas = pd.DataFrame(helpers.execute_sql(session, f"SHOW SCHEMAS IN DATABASE {source_db}"))["name"].tolist()
        source_schemas = st.multiselect(label = "please select the schemas you'd like to pull data from", options= schemas)

    if len(source_schemas) > 0:
        st.markdown("""---""")
        st.write("select tables here")
        st.markdown("""---""")
        table_dictionary = {}
        for i in source_schemas:
            tables = pd.DataFrame(helpers.execute_sql(session, f"SHOW TABLES IN SCHEMA {source_db}.{i}"))["name"].tolist()
            tables = [f"{source_db}.{i}." + s for s in tables]
            source_tables = st.multiselect(label = f"please select the tables you'd like to pull data from in schema {source_db}.{i}", options= tables)
            table_dictionary[f"{source_db}.{i}"] = source_tables
        
    if len(table_dictionary) > 0:
        st.markdown("""---""")
        st.write("select columns here")
        st.markdown("""---""")
        column_dictionary = {}
        tables = []
        for i in source_schemas:
            schema = f"{source_db}.{i}"
            tables.extend(table_dictionary[schema])
        print(tables)
        for i in tables:
            columns = pd.DataFrame(helpers.execute_sql(session, f"SHOW COLUMNS IN TABLE {i}"))["column_name"].tolist()
            source_columns = st.multiselect(label = f"please select the columns you'd like to pull data from in table {i}", options= columns)
            column_dictionary[f"{i}"] = source_columns
        print(column_dictionary)

    if len(research_group_name) > 0 and len(research_project) > 0 and len(wh_size) > 0 and len(users) > 0 and len(source_db) > 0 and len(source_db) > 0 and len(source_schemas) > 0 and len(table_dictionary) > 0 and len(column_dictionary) > 0:
        object_queries = object_query_generation()
        privilege_queries = privilege_query_generation()
        
        # object_status = ["Not Run"] * len(object_queries)
        # print(object_status)
        # object_queries_df = pd.DataFrame({
        #     'Query': object_queries,
        #     'Status': object_status
        # })

        if 'button1_clicked' not in st.session_state:
            st.session_state.button1_clicked = False

        if 'button2_clicked' not in st.session_state:
            st.session_state.button2_clicked = False

        if not st.session_state.button1_clicked:
            if st.button("Run Snowflake Object Creation Scripts"):
                object_queries_df = helpers.query_executions(session = session, queries = object_queries)
                helpers.save_env(environment_name, fr_name, wh_name)
                st.table(object_queries_df)
                st.write("Please ensure all queries successfully ran before continuing")
                st.session_state.button1_clicked = True

        if not st.session_state.button2_clicked and st.session_state.button1_clicked == True:
            if st.button("Run Snowflake Grants Scripts"):
                grants_queries_df = helpers.query_executions(session = session, queries = privilege_queries)
                st.table(grants_queries_df)
                st.write("Please ensure all queries successfully ran, if you'd like to stand up a new environment, hit the add new environment file")
                if st.button("Start a new environment"):
                    for key in st.session_state.keys():
                        del st.session_state[key]
                    st.rerun()
                st.session_state.button2_clicked = True
        # if st.button("Run Snowflake Object Creation Scripts"):
        #     object_queries_df = helpers.query_executions(session = session, queries = object_queries)
        #     helpers.save_env(environment_name, fr_name, wh_name)
        #     st.table(object_queries_df)
        #     st.write("Please ensure all queries successfully ran before continuing")
        #     if st.button("Run Snowflake Grants Scripts"):
        #         grants_queries_df = helpers.query_executions(session = session, queries = object_queries)
        #         st.table(grants_queries_df)
        #         st.write("Please ensure all queries successfully ran, if you'd like to stand up a new environment, hit the add new environment file")
        
        # if st.button("Run Object Creation Scripts"):
        #     object_status = []
        #     for i in object_queries:
        #         try:
        #             helpers.execute_sql(session, i)
        #             object_status.append("Query Succeeded")
        #         except Exception as e:
        #             object_status.append("Query Failed")
        #     object_queries_df = pd.DataFrame({
        #         'Query': object_queries,
        #         'Status': object_status
        #     })

        #     st.table(object_queries_df)
        #     st.write("Please ensure all queries successfully ran before continuing")
            # if object_queries_df["Status"].eq("Query Succeeded").all():
            #     st.write("All object creations succeeded")

        # if st.button("Run Snowflake Grants Scripts"):
        #     object_status = []
        #     for i in object_queries:
        #         try:
        #             helpers.execute_sql(session, i)
        #             object_status.append("Query Succeeded")
        #         except Exception as e:
        #             object_status.append("Query Failed")
        #     object_queries_df = pd.DataFrame({
        #         'Query': object_queries,
        #         'Status': object_status
        #     })

        #     st.table(object_queries_df)
        #     st.write("Please ensure all queries successfully ran before continuing")
            # if object_queries_df["Status"].eq("Query Succeeded").all():
            #     st.write("All object creations succeeded")



        
        # if st.button("run object creation scripts"):
        #     for i in object_queries:
        #         try:
        #             st.write(helpers.execute_sql(session, i))
        #         except Exception as e:
        #             st.write(e)
                
        # st.write("------------------")
        # for i in privilege_queries:
        #     st.write(i)
    
