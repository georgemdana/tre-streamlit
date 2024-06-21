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

# Find all environments set up through Frost

session = helpers.create_snowpark_session(username, password, account, role, warehouse)
environment_management, environment_test = st.tabs(["Environment Manager", "Environment Test"])
object_status = "not ran"

with environment_management:
    environments = [ f for f in os.listdir("environments/") if f.endswith('.yaml') ]
    if len(environments) == 0:
        st.write("**No Environments Detected. Please create an environment to get started.**")
    else:
        col1, col2, col3 = st.columns(3)
        #boxed_text = f"### **{ 'Total TREs: :red[" + 'str(len(set(environments)))' + "]'}**"
        #col1.markdown(boxed_text, unsafe_allow_html=True)
        col1.markdown("**Total TREs: :red[" + str(len(set(environments))) + "]**")
        # col3.write(f"{tre_count_header :>30}")
        # col3.write(f"'{:>30}'.format({tre_count_header})")

        # with col3:
        #     tre_count_header = "**Total TR Environments: :red[" + str(len(set(environments))) + "]**"
        #     rightalign = ":>25"
        #     st.write(f"{Total TR Environments: " + str(len(set(environments))) + ": >25}")

        for environment in environments:
            db_name, fr_name, wh_name, users, date_created, archive_status, archive_date = helpers.read_data(environment)

            col1, col2 = st.columns(2)
            with col1:
                newline = '  \n'
                #st.write(f"**Environment Name:** {environment.split('.')[0]}")
                st.subheader(f":blue[{environment.split('.')[0]}]")
                # st.write(f"**Functional Role:** {fr_name}")
                # st.write(f"**Warehouse:** {wh_name}")
                # st.write(f"**Users:** {users}")
                # st.write(f"**Date Created:** {date_created}")
                # st.write(f"**Archive Status:** {archive_status}")
                # st.write(f"**Archive Date:** {archive_date}")
                st.write(f"**Functional Role:** {fr_name} {newline}"
                f"**Warehouse:** {wh_name} {newline}"
                f"**Users:** {users} {newline}"
                f"**Date Created:** {date_created} {newline}"
                f"**Archive Status:** {archive_status} {newline}"
                f"**Archive Date:** {archive_date}"
                )

            with col2:
                st.write("")
                st.write("")
                st.write("")
                st.write("")
                st.write("")
                st.write("")
                st.write("")
                if st.button("Delete Environment", key = environment):
                    delete_statements = helpers.delete_environment(environment)
                    for statement in delete_statements:
                        helpers.execute_sql(session, statement)
                    os.remove(f"environments/{environment}")
                    st.experimental_rerun()
                if archive_status == 'False':
                    if st.button("Archive Environment", key = f"{environment}_archive"):
                        archive_statements = helpers.archive_environment(environment)
                        for statement in archive_statements:
                            helpers.execute_sql(session, statement)
                        with open(f"environments/{environment}", "r") as file:
                            data = yaml.safe_load(file)
                        
                        data['archived'] = 'True'
                        data['archive_date'] = date.today()

                        with open(f"environments/{environment}", "w") as file:
                            yaml.dump(data, file)
                        st.experimental_rerun()

            st.divider()


with environment_test:
    st.write("")
    st.write("")
    st.write(f"**Under Construction**")