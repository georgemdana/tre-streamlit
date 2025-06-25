# Trusted Research Environment 

## Streamlit App Overview:

### 1. Environment Creation Tab:
- Allows users to create a new research environment in Snowflake.
- Asks for details like research group name, research project name, warehouse size, table privileges, users, source database, schemas, tables, and columns.
- Generates SQL queries for creating databases, schemas, tables, and functional roles based on user input.
- Executes the generated SQL queries.
- Displays the execution status of queries and prompts users to ensure their successful completion before proceeding.

### 2. Environment Management Tab:
- Provides options for managing existing environments.
- Lists the currently saved environments.
- Allows users to delete environments, which executes SQL delete statements and removes associated files.

### General:
- Utilizes Streamlit for building the user interface.
- Uses Snowpark for executing Snowflake SQL commands.
- Employs helper functions for executing SQL commands, saving environment details, and managing environments.

## Helpers Functions Overview

### 1. `create_snowpark_session(username, password, account, role = "ACCOUNTADMIN", warehouse = "COMPUTE_WH")`:
- **Purpose**: Creates a Snowpark session for executing Snowflake SQL commands.
- **Parameters**: 
  - `username`: Snowflake username.
  - `password`: Snowflake password.
  - `account`: Snowflake account name.
  - `role` (optional, default: "ACCOUNTADMIN"): Snowflake role.
  - `warehouse` (optional, default: "COMPUTE_WH"): Snowflake warehouse.
- **Returns**: Snowpark session object.

### 2. `execute_sql(session, command)`:
- **Purpose**: Executes SQL commands using the provided Snowpark session.
- **Parameters**: 
  - `session`: Snowpark session object.
  - `command`: SQL command to execute.
- **Returns**: Execution result.

### 3. `execute_sql_pandas(session, command)`:
- **Purpose**: Executes SQL commands using the provided Snowpark session and returns the result as a Pandas DataFrame.
- **Parameters**: 
  - `session`: Snowpark session object.
  - `command`: SQL command to execute.
- **Returns**: Execution result as a Pandas DataFrame.

### 4. `querify_list(list)`:
- **Purpose**: Converts a list into a comma-separated string.
- **Parameters**: 
  - `list`: List of items.
- **Returns**: Comma-separated string.

### 5. `save_env(environment_name, fr_name, wh_name)`:
- **Purpose**: Saves environment details such as environment name, functional role (FR) name, and warehouse (WH) name to a YAML file.
- **Parameters**: 
  - `environment_name`: Name of the environment.
  - `fr_name`: Functional role name.
  - `wh_name`: Warehouse name.

### 6. `read_data(environment_name)`:
- **Purpose**: Reads environment details from a YAML file.
- **Parameters**: 
  - `environment_name`: Name of the environment.
- **Returns**: Environment details (database name, FR name, WH name).

### 7. `delete_environment(environment_name)`:
- **Purpose**: Deletes an environment by generating SQL delete statements.
- **Parameters**: 
  - `environment_name`: Name of the environment.
- **Returns**: List of SQL delete statements.

### 8. `query_executions(session, queries)`:
- **Purpose**: Executes a list of SQL queries using the provided Snowpark session and generates a DataFrame summarizing the execution status of each query.
- **Parameters**: 
  - `session`: Snowpark session object.
  - `queries`: List of SQL queries to execute.
- **Returns**: DataFrame summarizing the execution status of each query.

## Objects Function Overview

### 1. `db_standup_query_gen(research_group_name, research_project)`:
- **Purpose**: Generates a SQL query for creating a database for a given research group and project.
- **Parameters**: 
  - `research_group_name`: Name of the research group.
  - `research_project`: Name of the research project.
- **Returns**: Tuple containing the environment name and the SQL query for creating the database.

### 2. `schema_standup_query_gen(schema, environment_name)`:
- **Purpose**: Generates a SQL query for creating a schema within a specified environment.
- **Parameters**: 
  - `schema`: Name of the schema.
  - `environment_name`: Name of the environment where the schema will be created.
- **Returns**: SQL query for creating the schema.

### 3. `table_standup_query_gen(table, environment_name, schema, source_db, table_columns)`:
- **Purpose**: Generates a SQL query for creating a table within a specified environment and schema.
- **Parameters**: 
  - `table`: Name of the table.
  - `environment_name`: Name of the environment where the table will be created.
  - `schema`: Name of the schema where the table will be created.
  - `source_db`: Name of the source database where the table data is sourced from.
  - `table_columns`: List of column names for the table.
- **Returns**: SQL query for creating the table.

### 4. `functional_role_standup_query_gen(fr_name)`:
- **Purpose**: Generates a SQL query for creating a functional role.
- **Parameters**: 
  - `fr_name`: Name of the functional role.
- **Returns**: SQL query for creating the functional role.

### 5. `warehouse_standup_query_gen(wh_name, wh_size)`:
- **Purpose**: Generates a SQL query for creating a warehouse with a specified size.
- **Parameters**: 
  - `wh_name`: Name of the warehouse.
  - `wh_size`: Size of the warehouse.
- **Returns**: SQL query for creating the warehouse.

## Policies Function Overview

### 1. `fr_privilege_statement_gen(session, fr_name, privileges_list, environment_name)`:
- **Purpose**: Generates SQL statements to grant privileges to a functional role (FR) within a specified environment.
- **Parameters**: 
  - `session`: Snowpark session object.
  - `fr_name`: Name of the functional role.
  - `privileges_list`: List of privileges to be granted.
  - `environment_name`: Name of the environment.
- **Returns**: List of SQL statements to grant privileges.

### 2. `fr_assignment_statement_gen(privileges_statements, fr_name, users)`:
- **Purpose**: Generates SQL statements to assign a functional role (FR) to users.
- **Parameters**: 
  - `privileges_statements`: List of SQL statements with privileges.
  - `fr_name`: Name of the functional role.
  - `users`: List of users to whom the FR will be assigned.
- **Returns**: List of SQL statements with privileges and FR assignments.

### 3. `wh_assignment_statement_gen(privileges_statements, fr_name, wh_name)`:
- **Purpose**: Generates SQL statements to assign a warehouse (WH) to a functional role (FR).
- **Parameters**: 
  - `privileges_statements`: List of SQL statements with privileges and FR assignments.
  - `fr_name`: Name of the functional role.
  - `wh_name`: Name of the warehouse.
- **Returns**: List of SQL statements with privileges, FR assignments, and WH assignments.
