# import streamlit as st
# from pages import home
# from pages import streamlit_main
# from pages import sigma
# from pages import cohortbuilder

# #def main():
#     #st.sidebar.title("Navigation")
#     #choice = st.sidebar.radio("Select a page:", list(PAGES.keys()))

#     #if choice in PAGES:
#     #    PAGES[choice].main()

# PAGES = {
#     "Home": home,
#     "Frost App": streamlit_main,
#     "Dashboards": sigma,
#     "Cohort Builder": cohortbuilder
# }



# if __name__ == "__main__":
#     main()


import streamlit as st

def frosthomepage():
    frostmarkdown = """

        ## Welcome!
        *Frost is a no code solution to building out a Trusted Research Environment on Snowflake.*

        Frost is a self service tool to help secure your data to conduct research at scale. Giving you the options to choose your warehouse, database, schemas, users and accessibility to build out your research cohorts, safely. Here are just a few benefits of Frost:

        - Security
        - Easy of Access
        - Collaboration
        - Scale
        - Transparency
        - Results
        - Analytics

        ### Get Started Guide
    
        ##### 1. Environment Builder Tab
        Easily stand up a Trusted Research Environment in Snowflake. This environment will be a skeleton with no data.
        ##### 2. Environment Manager Tab
        Data about your Trusted Research Environments. This includes schemas, tables, roles, warehouses, users, creation date. You can also use this tab to delete and archive environments.
        ##### 3. Cohort Builder Tab
        Use the skeleton environment you just stood up in step 1 to choose the precise cohort of data you would like added for your research purposes.
        ##### 4. Dashboards Tab
        Start building dashboards in Sigma to begin analyzing your research cohort.

    """
    
    st.markdown(frostmarkdown)

def main():
    ## App Start
    ## App Start
    st.image('frostbanner.png', width = 300)
    st.image('frostlogo.png', width = 300)
    frosthomepage()

if __name__ == "__main__":
    main()