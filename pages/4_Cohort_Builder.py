import streamlit as st
import sys
sys.path.append('./src')
from helpers import helpers
from objects import objects
from policies import policies
import pandas as pd
import os
from datetime import date
import yaml
from streamlit_dynamic_filters import DynamicFilters
from streamlit import session_state

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
st.image('frostlogo.png', width = 300)
st.caption("Trusted Research Environment Set Up Tool")

session = helpers.create_snowpark_session(username, password, account, role, warehouse)
source_tables = []
source_columns = []
tables = []
filtered_cohort_build = ""
if 'source_db' not in st.session_state:
    st.session_state.source_db = {}
if 'source_schemas' not in st.session_state:
    st.session_state.source_schemas = {}
if 'source_tables' not in st.session_state:
    st.session_state.source_tables = {}
if 'source_columns' not in st.session_state:
    st.session_state.source_columns = {}
if 'source_roles' not in st.session_state:
    st.session_state.source_roles = {}
if 'cohort_status' not in st.session_state:
    st.session_state.cohort_status = {}
if 'filter_state' not in st.session_state:
    st.session_state['filter_state'] = {}


cohort_builder, cohort_check = st.tabs(["Cohort Builder", "Cohort Check"])

with cohort_builder:
    helpers.execute_sql(session, "USE ROLE SYSADMIN")
    st.subheader('Name Your Cohort')
    cohort_name = st.text_input(label = "Cohort Name") # No spaces
    ch_name = f"FR_{cohort_name.upper()}"
    tb_name = f"TRE_{cohort_name.upper()}"
    if len(cohort_name) > 0:
        st.subheader('Choose Your TRE, Schema, Tables and Columns')
    # Choose environment/database
        environments = [ f for f in os.listdir("environments/") if f.endswith('.yaml') ]
        if len(environments) == 0:
            st.write("**No Environments Detected. Please use the Environment Builder Tab to Get Started.**")
        else:
            source_db = st.selectbox(label = "Trusted Research Environment", options = helpers.strip_yaml(environments))
            table_dictionary = {}

        # schema selection wizard
        if len(source_db) > 0:
            schemas = pd.DataFrame(helpers.execute_sql(session, f"SHOW SCHEMAS IN DATABASE {source_db}"))["name"].tolist()
            source_schemas = st.multiselect(label = "Source Schemas", options = schemas)

        # table selection wizard
        if len(source_db) > 0 and len(source_schemas) > 0:
            table_dictionary = {}
            for i in source_schemas:
                tables = pd.DataFrame(helpers.execute_sql(session, f"SHOW TABLES IN SCHEMA {source_db}.{i}"))["name"].tolist()
                tables = [f"{source_db}.{i}." + s for s in tables]
                #source_tables = st.multiselect(label = f"Source Tables From Schema: {source_db}.{i}", options = tables)
                source_tables = st.selectbox(label = f"Source Tables", options = tables)
                source_tables_dict = []
                table_dictionary[f"{source_db}.{i}"] = source_tables_dict
                table_status = "ran"
            
        # column selection wizard
        if len(source_db) > 0 and len(source_schemas) > 0 and table_status == "ran":
            column_dictionary = {}
            #tables = []
            roles = []
            for i in source_schemas:
                schema = f"{source_db}.{i}"
                tables.extend(table_dictionary[schema])
            print(tables)
            for i in tables:
                columns = pd.DataFrame(helpers.execute_sql(session, f"SHOW COLUMNS IN TABLE {i}"))["column_name"].tolist()
                source_columns = st.multiselect(label = f"Source Columns From Table: {i}", options = columns)
                column_dictionary[f"{i}"] = source_columns
            print(column_dictionary)

        # User Role selection wizard
            roles = pd.DataFrame(helpers.execute_sql(session, f"SHOW ROLES"))["name"].tolist()
            source_roles = st.selectbox(label = f"User Role", options = roles)

        # Save to session
        if len(source_db) > 0 and len(source_schemas) > 0 and len(table_dictionary) > 0 and len(source_roles) > 0:
            st.session_state.source_db = source_db
            st.session_state.source_schemas = source_schemas
            st.session_state.source_tables = source_tables
            st.session_state.source_columns = source_columns
            st.session_state.source_roles = source_roles
            st.session_state.cohort_status = "ran"    


# Dynamic Filter Wizard
        environments = [ f for f in os.listdir("environments/") if f.endswith('.yaml') ]
        if len(environments) == 0:
            st.write("**No Environments Detected. Please create an environment to get started.**")
        else:
            if len(st.session_state.source_columns) > 0 and len(st.session_state.source_roles) > 0 and st.session_state.cohort_status == "ran":
                st.header("Filters:")

        # # Update the session state with the selected columns
        # st.session_state.filter_state['table'] = st.session_state.source_tables
        # st.session_state.filter_state['columns'] = st.session_state.source_columns
        # st.write(st.session_state.filter_state['table'])

        for table in st.session_state.source_tables:
            helpers.execute_sql(session, f"USE ROLE {st.session_state.source_roles}")
            helpers.execute_sql(session, f"USE WAREHOUSE {warehouse}")
            data = pd.DataFrame(helpers.execute_sql(session, f"SELECT * FROM {st.session_state.source_tables}"))
        
        filters = {}
        for col in st.session_state.source_columns:
            unique_values = data[col].unique().tolist()
            unique_values.insert(0, "All")
            selected_value = st.multiselect(col, [''] + unique_values)
            unique_values.remove("All")
            if "All" in selected_value:
                selected_value = list(unique_values)
            if selected_value:
                filters[col] = selected_value

        if st.button("Apply Filters and Preview Cohort"):
            # Create a Snowflake query with filters
            query = f"SELECT * FROM {st.session_state.source_tables}"
            if any(filters.values()):
                query += " WHERE "
                for col, filter_val in filters.items():
                    if filter_val:
                        if isinstance(filter_val, list):
                            # Use a list in the WHERE clause for multiple values
                            val_list = ', '.join(f"'{val}'" for val in filter_val)
                            query += f"{col} IN ({val_list}) AND "
                        else:
                            query += f"{col} = '{filter_val}' AND "
                query = query.rstrip(" AND ")
                   

            result = helpers.execute_sql(session, query)
            filtered_cohort = pd.DataFrame(result)
            st.write(filtered_cohort)
            filtered_cohort_build = "ran"

        if filtered_cohort_build == "ran":
            if st.button("Create New Cohort"):
                # Create a new table in Snowflake
                cols = list(filtered_cohort.columns)
                helpers.create_table_in_snowflake(session, tb_name, "TRE", "PUBLIC", cols)

                # Load the Pandas DataFrame into Snowflake
                helpers.load_df_to_snowflake(session, filtered_cohort, st.session_state.source_tables, st.session_state.source_schemas)

                # column_dtype_dict = filtered_cohort.dtypes.astype(str).to_dict()
                # create_new_sf_table(session, tb_name, column_dtype_dict, "TRE", "PUBLIC")
                st.write(f"New Cohort Created in {st.session_state.source_db} as {tb_name}")

    else:
        st.write("Try Again")

        
# ############ ------------------------------------------------------------------------------------------------------------------------------------------------
# # dynamic filter wizard
#     environments = [ f for f in os.listdir("environments/") if f.endswith('.yaml') ]
#     if len(environments) == 0:
#         st.write("**No Environments Detected. Please create an environment to get started.**")
#     else:
#         if len(st.session_state.source_columns) > 0 and len(st.session_state.source_roles) > 0 and st.session_state.cohort_status == "ran":
#             st.subheader(f"Filters")
#             for environment in environments:
#                 db_name, fr_name, wh_name, users, date_created, archive_status, archive_date = helpers.read_data(environment)

#             # Get the list of columns from Snowflake
#             columns = pd.DataFrame(helpers.execute_sql(session, f"SHOW COLUMNS IN TABLE TRE_DISCHARGE_DISCHARGE_PROJ.PUBLIC.DISCHARGE_PROCESSED"))["column_name"].tolist()
#             #cursor.execute("SHOW COLUMNS IN TABLE your_table_name")
#             #columns = [row[0] for row in cursor.fetchall()]

#             # Create a dropdown menu for selecting columns
#             #columns = source_tables.columns.tolist()
#             #selected_columns = st.multiselect("Select columns for filtering:", source_columns, default=columns[:3])

#             # Create two columns in Streamlit
#             col1, col2 = st.columns(2)

#             # Create dynamic columns for filters in col1
#             with col1:
#                 for table in tables:
#                     helpers.execute_sql(session, f"USE ROLE {source_roles}")
#                     helpers.execute_sql(session, f"USE WAREHOUSE {warehouse}")
#                     data = pd.DataFrame(helpers.execute_sql(session, f"SELECT * FROM {table}"))
#                 st.header("Filters:")
#                 filters = {}
#                 for col in columns:
#                     unique_values = data[col].unique().tolist()
#                     unique_values.insert(0, "All")
#                     selected_value = st.multiselect(col, [''] + unique_values)
#                     if "All" in selected_value:
#                         selected_value = list(unique_values)
#                     if selected_value:
#                         filters[col] = selected_value

#             # Create a button to apply the filters in col2
#             with col2:
#                 if st.button("Apply Filters"):
#                     # Create a Snowflake query with filters
#                     query = "SELECT * FROM TRE_DISCHARGE_DISCHARGE_PROJ.PUBLIC.DISCHARGE_PROCESSED"
#                     if any(filters.values()):
#                         query += " WHERE "
#                         for col, filter_val in filters.items():
#                             if filter_val:
#                                 query += f"{col} = '{filter_val}' AND "
#                         query = query.rstrip(" AND ")
#                     result = helpers.execute_sql(session, query)
#                     df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
#                     st.write(df)

############ ------------------------------------------------------------------------------------------------------------------------------------------------

            ## ----------------------------------------

            # # Create dynamic columns based on the selected columns
            # dynamic_cols = []
            # for col in selected_columns:
            #     dynamic_cols.append(st.columns(f"Filter {col}"))
            #     filter_val = dynamic_cols[-1].text_input(f"Filter {col}:", value="")
            #     if filter_val:
            #         filter_query = f"SELECT * FROM {source_tables} WHERE {col} = '{filter_val}'"
            #         cursor.execute(filter_query)
            #         result = cursor.fetchall()
            #         dynamic_cols[-1].write(result)

            # # Run the app
            # if st.button("Run"):
            #     st.write("Filters applied!")

           # ---------------------------
            
            # for schemas in source_schemas:
            #     schema = f"{source_db}.{schemas}"
            #     tables.extend(table_dictionary[schema])
            # print(tables)
            # for table in tables:
            #     helpers.execute_sql(session, f"USE ROLE {source_roles}")
            #     helpers.execute_sql(session, f"USE WAREHOUSE {warehouse}")
            #     data = pd.DataFrame(helpers.execute_sql(session, f"SELECT * FROM {table}"))

            #     # Create a dictionary to store the filter criteria
            #     filters = {}

            #     # Create a dropdown filter for each column
            #     for source_columns in data.columns:
            #         unique_values = data[source_columns].unique().tolist()
            #         unique_values.insert(0, "All")
            #         selected_value = st.multiselect(f'Select a value for {source_columns}:', [''] + unique_values, key = {source_columns}, default = columns[:3])
            #         if "All" in selected_value:
            #             selected_value = list(unique_values)
            #         if selected_value:
            #             filters[source_columns] = selected_value

            #     # Run the app
            #     if st.button("Run"):
            #         st.write("Filters applied!")

            #     # Create a button to apply the filters
            #     apply_filters_button = st.button('Apply Filters')

            #     # Define a function to apply the filters
            #     def apply_filters(data, filters):
            #         filtered_data = data.copy()
            #         for column, filter_value in filters.items():
            #             filtered_data = filtered_data[filtered_data[column] == filter_value]
            #         return filtered_data

            #     # Apply the filters when the button is clicked
            #     if apply_filters_button:
            #         filtered_data = apply_filters(data, filters)
            #         st.write(filtered_data)


                # ---------------------------

                # column_selector = st.selectbox('Select a column:', environmentdata.columns)
                # filter_input = st.text_input('Enter a filter value:')
                # filter_button = st.button('Apply Filter')

                # def filter_data(data, column, filter_value):
                #     filtered_data = data[data[column].str.contains(filter_value)]
                #     return filtered_data

                # if filter_button:
                #     filtered_data = filter_data(environmentdata, column_selector, filter_input)
                #     st.write(filtered_data)





                # columns = []
                # columns = pd.DataFrame(helpers.execute_sql(session, f"SHOW COLUMNS IN TABLE {table}"))["column_name"].tolist()
                # environmentdata[columns] = environmentdata[columns].astype(str)
                # dynamic_filters = DynamicFilters(environmentdata, filters=['NOTE_ID', 'CHARTTIME', 'DC_DIAGNOSIS', 'SEX', 'CHIEF_COMPLAINT', 'SERVICE', 'DISCHARGE_DISPOSITION', 'PMH_CARDIOVASCULAR_DISEASE', 'PMH_DIABETES_MELLITUS'])
                # dynamic_filters.display_filters(location='columns', num_columns=2, gap='large')

                #column_dictionary[f"_{schemas}"] = columns
                #dynamic_filters = DynamicFilters(environmentdata, filters=columns)
                #dynamic_filters.display_filters(location='columns', num_columns=2, gap='small')
                # dynamic_filters.display_df()

                #helpers.display_df_and_return(environmentdata, columns)
                #filtered_data = pd.DataFrame(st.session_state['filters'])
                # Assume 'filters' is a dictionary containing the filter settings
                
                #st.write(cohort_data)
                #cohort_data = cohort_data.to_dataframe()

                #cohort_data = dynamic_filters.filter_df()
                #cohort_columns_dict = {col: str(cohort_data[col].dtype) for col in cohort_data.columns}
                #cohort_columns_dict = {k: 'varchar' if v == 'object' else v for k, v in cohort_columns_dict.items()}

                #st.write(filtered_df)

                #st.write("A new table has been added to your Snowflake TRE")
                #helpers.create_new_sf_table(session, {cohort_name}, cohort_columns_dict)

        # else:
        #     st.write(" ")

############################################################# working code #################################################################
    # # dynamic filter wizard
    # col1, col2, col3 = st.columns(3)
    # with col1:
    #     # find data set columns to create filters
    #     if len(st.session_state.source_columns) > 0 and len(st.session_state.source_roles) > 0 and st.session_state.cohort_status == "ran":
    #         unique_col_vals = []
    #         tables = []
    #     for schemas in source_schemas:
    #         schema = f"{source_db}.{schemas}"
    #         tables.extend(table_dictionary[schema])
    #     print(tables)
    #     for table in tables:
    #         columns = pd.DataFrame(helpers.execute_sql(session, f"SHOW COLUMNS IN TABLE {table}"))["column_name"].tolist()
    #         column_dictionary[f"_{schemas}"] = columns
    #         print(column_dictionary)
    #         for i in column_dictionary:
    #             st.subheader(f"Filters")
    #             helpers.execute_sql(session, f"USE ROLE {source_roles}")
    #             column_unique_vals[f"_{i}"] = pd.DataFrame(helpers.execute_sql(session, f"SELECT DISTINCT {i} from {table}")).tolist()
    #             column_unique_vals[f"_{i}"] = column_unique_vals[f"_{i}"].insert(0, "Select All")
    #             filter[f"_{i}"] = st.multiselect(label = f"Filter By: {i}", options = column_unique_vals[f"_{i}"])
    #             column_dictionary[f"{i}"] = filter[f"_{i}"]
    #             print(column_dictionary)

            # for i in source_columns:
            #     st.subheader(f"Filters")
            #     column_unique_vals[f"_{i}"] = pd.DataFrame(helpers.execute_sql(session, f"SELECT DISTINCT {i} from {tables}")).tolist()
            #     column_unique_vals[f"_{i}"] = column_unique_vals[f"_{i}"].insert(0, "Select All")
            #     filter[f"_{i}"] = st.multiselect(label = f"Filter By: {i}", options = column_unique_vals[f"_{i}"])
            #     column_dictionary[f"{i}"] = filter[f"_{i}"]
            # print(column_dictionary)

#            for i in st.session_state.source_columns:
                #filter[f"{i}"] = pd.DataFrame(helpers.execute_sql(session, f"SHOW COLUMNS IN TABLE"))["name"].tolist()
                #helpers.execute_sql(session, f"USE ROLE {st.session_state.source_roles}")
                #column_unique_vals = pd.DataFrame(f"{st.session_state.source_tables}.{i}").unique
                #for tables in source_tables:
                    #column_unique_vals[f"_{i}"] = pd.DataFrame(helpers.execute_sql(session, f"SELECT DISTINCT {i} from {source_db}.{source_tables}"))[f"{i}"].tolist()
                #column_unique_vals[f"_{i}"] = pd.DataFrame(helpers.execute_sql(session, f"SELECT DISTINCT {i} FROM {st.session_state.source_db}.{st.session_state.source_tables}"))["column_values"].tolist()
                    #st.header(f"Filter By: {i}")
                #filter_options = pd.DataFrame(helpers.execute_sql(session, f"SELECT DISTINCT {i} FROM {st.session_state.source_tables}")).tolist()
                    #column_unique_vals[f"_{i}"] = column_unique_vals[f"_{i}"].insert(0, "Select All")
                    #filter[f"_{i}"] = st.multiselect(label = f"Filter By: {i}", options = column_unique_vals[f"_{i}"])
                #filter[f"{i}"] = st.multiselect(label = f"{i}", options = filter_options) # To be given access
                #if "Select All" in filter[f"{i}"]:
                #    filter[f"{i}"] = filter_options




    # # table selection wizard
    # if len(source_schemas) > 0:
    #     table_dictionary = {}
    #     for i in source_schemas:
    #         tables = pd.DataFrame(helpers.execute_sql(session, f"SHOW TABLES IN SCHEMA {dbs}.{i}"))["name"].tolist()
    #         tables = [f"{dbs}.{i}." + s for s in tables]
    #         source_tables = st.multiselect(label = f"Source Tables From Schema: {dbs}.{i}", options = tables)
    #         table_dictionary[f"{dbs}.{i}"] = source_tables
        
    # # column selection wizard
    # if len(table_dictionary) > 0:
    #     column_dictionary = {}
    #     tables = []
    #     for i in source_schemas:
    #         schema = f"{dbs}.{i}"
    #         tables.extend(table_dictionary[schema])
    #     print(tables)
    #     for i in tables:
    #         columns = pd.DataFrame(helpers.execute_sql(session, f"SHOW COLUMNS IN TABLE {i}"))["column_name"].tolist()
    #         source_columns = st.multiselect(label = f"Source Columns From Table: {i}", options= columns)
    #         column_dictionary[f"{i}"] = source_columns
    #     print(column_dictionary)

    #     dynamic_filters = DynamicFilters(source_tables, filters = source_columns)
    #     dynamic_filters.display_filters(location='columns', num_columns=4, gap='small')
    #     dynamic_filters.display_df()


    # df = pd.DataFrame(helpers.execute_sql(session, f"SHOW TABLES IN ")["name"].tolist()
    # columns = pd.DataFrame(helpers.execute_sql(session, f"SHOW COLUMNS IN TABLE {i}"))["column_name"].tolist()
    # dynamic_filters = DynamicFilters(df, filters=['region', 'country', 'city', 'district'])



with cohort_check:
    st.write("")
    st.write("")
    st.write(f"**Under Construction**")

# def object_query_generation():
#     ## this function cycles through all schemas under our chosen database and generates queries for db, schema, table and FR creation
#     object_queries = ["USE ROLE SYSADMIN"]
#     environment_name, envrionment_query = objects.db_standup_query_gen(research_group_name, research_project) #generate database create statement
#     object_queries.append(envrionment_query)

#     warehouse_query = objects.warehouse_standup_query_gen(wh_name, wh_size)
#     object_queries.append(warehouse_query)

#     for schema in source_schemas:
#         schema_query = objects.schema_standup_query_gen(schema, environment_name)
#         object_queries.append(schema_query)
#         tables = table_dictionary[f"{source_db}.{schema}"]
#         # tables = globals()[f"{schema}_tables"]
#         for table in tables:
#             table_query = objects.table_standup_query_gen(table, environment_name, schema, source_db, column_dictionary[f"{table}"])
#             object_queries.append(table_query)
#     object_queries.append("USE ROLE SECURITYADMIN")
#     functional_role_query = objects.functional_role_standup_query_gen(fr_name) #generate functional role creation query
#     object_queries.append(functional_role_query)
#     return object_queries

# # use this function to generate all queries needed for grant stand-up and return them as a list
# def privilege_query_generation():
#     environment_name, environment_query = objects.db_standup_query_gen(research_group_name, research_project)
#     statements = policies.fr_privilege_statement_gen(session, fr_name, privileges, environment_name)
#     statements_2 = policies.fr_assignment_statement_gen(statements, fr_name, users)
#     privilege_statements = policies.wh_assignment_statement_gen(statements_2, fr_name, wh_name)
#     return privilege_statements

# session = helpers.create_snowpark_session(username, password, account, role, warehouse)
# environment_creation, object_standup, grant_setup, environment_management = st.tabs(["Environment Creation", "Object Creation", "Grant Set Up", "Environment Management"])
# object_status = "not ran"

# with environment_creation:
#     helpers.execute_sql(session, "USE ROLE SYSADMIN")
#     research_group_name = st.text_input(label = "Research Group Name") # No spaces
#     research_project = st.text_input(label = "Research Project Name") # No spaces
#     fr_name = f"FR_{research_group_name.upper()}_{research_project.upper()}"
#     wh_name = f"WH_{research_group_name.upper()}_{research_project.upper()}"
#     environment_name = f"{research_group_name.upper()}_{research_project.upper()}_RG_ENVIRONMENT"
    
#     # fetch desired warehouse size
#     wh_sizes = ["XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE", "XXXLARGE", "X4LARGE", "X5LARGE", "X6LARGE"]
#     wh_size = st.selectbox(label = "Warehouse Size", options = wh_sizes)

#     # fetch privileges needed for users
#     privileges_options = ["SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES", "ALTER", "USAGE", "OWNERSHIP"]
#     privileges = st.multiselect(label = "Table Privileges", options = privileges_options)

#     # fetch users needed for research environment
#     users_options = pd.DataFrame(helpers.execute_sql(session, f"SHOW USERS"))["name"].tolist()
#     users = st.multiselect(label = "Users", options = users_options) # To be given access
    
#     dbs = pd.DataFrame(helpers.execute_sql(session, f"SHOW DATABASES"))["name"].tolist()
#     source_db = st.selectbox(label = "Source Database", options = dbs) # Select a source data base for this environment
#     table_dictionary = {}
    
#     # schema selection wizard
#     if len(source_db) > 0:
#         #st.markdown("""---""")
#         #st.write("Select schemas here")
#         #st.markdown("""---""")
#         schemas = pd.DataFrame(helpers.execute_sql(session, f"SHOW SCHEMAS IN DATABASE {source_db}"))["name"].tolist()
#         source_schemas = st.multiselect(label = "Source Schemas", options= schemas)

#     # table selection wizard
#     if len(source_schemas) > 0:
#         #st.markdown("""---""")
#         #st.write("Select tables here")
#         #st.markdown("""---""")
#         table_dictionary = {}
#         for i in source_schemas:
#             tables = pd.DataFrame(helpers.execute_sql(session, f"SHOW TABLES IN SCHEMA {source_db}.{i}"))["name"].tolist()
#             tables = [f"{source_db}.{i}." + s for s in tables]
#             source_tables = st.multiselect(label = f"Source Tables From Schema: {source_db}.{i}", options= tables)
#             table_dictionary[f"{source_db}.{i}"] = source_tables
        
#     # column selection wizard
#     if len(table_dictionary) > 0:
#         #st.markdown("""---""")
#         #st.write("Select columns here")
#         #st.markdown("""---""")
#         column_dictionary = {}
#         tables = []
#         for i in source_schemas:
#             schema = f"{source_db}.{i}"
#             tables.extend(table_dictionary[schema])
#         print(tables)
#         for i in tables:
#             columns = pd.DataFrame(helpers.execute_sql(session, f"SHOW COLUMNS IN TABLE {i}"))["column_name"].tolist()
#             source_columns = st.multiselect(label = f"Source Columns From Table: {i}", options= columns)
#             column_dictionary[f"{i}"] = source_columns
#         print(column_dictionary)

#     # query generation and execution wizard
#     if len(research_group_name) > 0 and len(research_project) > 0 and len(wh_size) > 0 and len(users) > 0 and len(source_db) > 0 and len(source_db) > 0 and len(source_schemas) > 0 and len(table_dictionary) > 0 and len(column_dictionary) > 0:
#         st.write("Wizard is successfully filled out. Please continue to the Object Creation tab.")

# with object_standup:
#     if len(research_group_name) > 0 and len(research_project) > 0 and len(wh_size) > 0 and len(users) > 0 and len(source_db) > 0 and len(source_db) > 0 and len(source_schemas) > 0 and len(table_dictionary) > 0 and len(column_dictionary) > 0:
#         object_status = "ran"
#         object_queries = object_query_generation()
#         st.header("Research Environment Object Query Standup and Execution")

#         st.write("Review the following queries and ensure they align with what you want to set up")
#         for query in object_queries:
#             st.write(query)
#         if st.button("Run Snowflake Object Creation Scripts"):
#             object_queries_df = helpers.query_executions(session = session, queries = object_queries)
#             helpers.save_env(environment_name, fr_name, wh_name, users, date.today(), "False")
#             st.table(object_queries_df)
#             run_variable = "exists"
#             st.write("Please ensure all queries successfully ran before continuing")
#     else:
#         st.write("Please fill out first tab before moving to this page")

# with grant_setup:
#     if len(research_group_name) > 0 and len(research_project) > 0 and len(wh_size) > 0 and len(users) > 0 and len(source_db) > 0 and len(source_db) > 0 and len(source_schemas) > 0 and len(table_dictionary) > 0 and len(column_dictionary) > 0 and object_status == "ran":
#         st.header("Research Environment Grant Query Standup and Execution")
#         try:
#             st.write("Review the following queries and ensure they align with what grants need set up")
#             privilege_queries = privilege_query_generation()
#         except:
#             st.write("No queries found, please ensure you've run all object standup before moving to this page")
#             privilege_queries = []
#         for query in privilege_queries:
#             st.write(query)
#         if st.button("Run Snowflake Grants Scripts"):
#             privilege_queries = privilege_query_generation()
#             grants_queries_df = helpers.query_executions(session = session, queries = privilege_queries)
#             st.table(grants_queries_df)
#             st.session_state.button2_clicked = True
#     else:
#         st.write("Please fill out second tab before moving to this page")

# with environment_management:
#     environments = [ f for f in os.listdir("environments/") if f.endswith('.yaml') ]
#     if len(environments) == 0:
#         st.write("No Environments Detected, Please Create an Environment to get Started")
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