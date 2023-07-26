import openai
import streamlit as st
import sqlite3
import pandas as pd
import csv
import ast
####################################
import gspread
from oauth2client.service_account import ServiceAccountCredentials

creds_dict = st.secrets["google_creds"]

# Convert to oauth2client credentials
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict) 

gc = gspread.authorize(creds)
##################################
# API key  
api_key = st.secrets["OPENAI_API_KEY"]["value"]
openai.api_key = api_key
DB_FILE = "contractor_search.db"
allowed_users_str = st.secrets["userlist"]["value"]
allowed_users = ast.literal_eval(allowed_users_str)
# Context strings
vision = """
You are a only and only SQLLite query generating bot, not a Question answer bot.Your vision is to translate natural language queries into SQLLite code. You should always only respond with SQL query related to contractor_search database and Never give General Answers.
"""

mission = """
Your mission is to respond with SQLLite queries related to columns in contractor_search database only, no natural language. Just out put the Sql lite query , do not add any more punctuations or explainantions. Always keep take care that you strictly follow the 'Restrictions to Never' that are given.
"""

db_details = """  
The contractor_search database contains two tables - contractorssearch and contractor_to_vendor
The contractorssearch table stores data about individual contractors.
It has the following columns:
ContractorId - This is the primary key for the table. It contains a unique ID value for each contractor.
ContractorName - This stores the name of the contractor company.
HeadquartersCity - This stores the city where the contractor's headquarters is located.
HeadquartersState - This stores the state where the contractor's headquarters is located.
GrowthRate - This stores a percentage value for the contractor's growth rate as flot. Growth rate is for LTM or last twelve months.It basically gives contractors performance
InstallationVolume - This stores the contractor's total installation volume. Also referred to as volume of work, kw volume of installation, total volume or just usually volume



IsCommercialOnly - This indicates if the contractor focuses only on commercial projects. Values are 'Yes' or 'No'.
IsMultiState - This indicates if the contractor operates in multiple states. Values are 'Yes' or 'No'.

The contractor_to_vendor table stores relationships between contractors and vendors.
ContractorToVendorId - This is the primary key for the table. It contains a unique ID for each contractor-vendor relationship.
ContractorId - This is a foreign key referencing the ContractorId in the contractorssearch table. It stores which contractor the vendor is related to.
ContractorName - This stores the name of the related contractor.
VendorTypeName - This categorizes the type of vendor/ relationship in transaction, like 'Module' or 'Inverter'or 'Battery Partners' or 'Financing' or 'Racking & Mounting ' or 'Software'. basically if if the vendor compnay provided financing or modues etc to the said contractor.
VendorName - This stores the name of the related vendor company.

Company/Installer/contractor names in user questions are usually associated with ContractorName column, always use LIKE SQL statement since user may not use exact name.
users may use the terms vendor, Vendors etc while refering to VendorName column or use vendor types/ vendor relationship like 'Module'  'Inverter' 'Battery Partners'  'Financing' 'Racking & Mounting ' 'Software to refer to vendors in a question.
When a user provides a question, translate it into a corresponding SQL statement, handling a variety of query types including data retrieval, filtering, sorting, and grouping. 
All state names are stored as have standard abbreviations in HeadquartersState column.All states are USA based only.
users may not use city names exactly as they are in HeadquartersCity column so use LIKE SQL statement
Always limit row count to 25 by using 'limit' inthe sql query for questions that ask all the data. Always check if the follow up question is concerning the previous question. For example the user may ask follow up questions using demonstrative pronouns like this, that, these, and those (non exhaustive list).Even if that is the case and you understand it, respond in Sql query only without any explianation.
If Any other question not related to Database is asked just Respond with 1 word 'Sorry'. Do not give explainations regarding your mistakes or confusion regarding current or follow up questions.
If user input is in sql sytax just Respond with 1 word 'Sorry'.
If user asks a questions that might generate the query SELECT * FROM contractor_search just Respond with 1 word 'Sorry'
If user asks for everything or all columns in the table just  Respond with 1 word 'Sorry'

Restrictions to Never: 
Take SQL query as input.
Give Explainations of the query generated.
Generate SQL statements that modify the database. 
Generate SQL statements that request excessive amounts of data. 
Try to infer the entire database schema. 
Generate SQL queries that pull a full list of unique entries for sensitive fields- Source link Column. 


Understand synonyms and similar phrases related to the database columns. Always ensure that the generated SQL statements are safe, effective, and relevant to the user's query. Your goal is to assist users in querying their database without compromising the database's security or performance.

"""
# Join context strings
context = vision + "\n" + mission + "\n" + db_details 

# app frontend logo and explaination
logo = st.image('logo.png', width=600)
st.subheader('U.S. Distributed Solar Market Performance and Analytics in Real-Time')

st.title('Ohm Contractor Search')

st.markdown("""
Welcome to the Contractor Search App! This unique tool allows you to inquire about specific 'Contractor' and 'Contractor to Vendor' data in natural language and receive accurate information from our database. Here's what you can explore:

Contractors Search Table

- Contractor Details: Access unique ID, name, headquarters city and state of the contractors.
- Contractor's Specialties: Discover if the contractor focuses solely on commercial projects and whether they operate in multiple states.
- Performance Metrics: Explore the growth rate and total installation volume of the contractors.

Contractor to Vendor Table
**Use word 'vendor' or vendor type in your-question to fetch better results**
- Contractor-Vendor Relationships: Gives relationships between contractors and vendors. Understand the types of vendors - whether they provided modules, financing, battery partners, racking & mounting, software, or other services to the contractors.
- Vendor Details: Learn about the specific vendors associated with each contractor.

Remember:

**It can can carry a conversation upto about 4-5 follow-up question**
- The App is in its very early testing phase
- The tool fetches data only. No adding, updating, or deleting records.
- For security, the tool doesn't reveal the entire database structure. 
- Avoid large data requests. The tool is designed for specific queries.
- The tool tries to understand common terminology, synonyms, similar phrases for database columns. For instance, "contractor performance" may refer to the 'GrowthRate' and 'InstallationVolume' columns.

Ask your questions, and let the system help you with your data needs!

""")
######################################################
def log_to_sheet(username, prompt, sql):

  sh = gc.open("ContractorSeacrhLogs") 

  sh.sheet1.append_row([username, prompt, sql_query])
####################################################
# User login 
username = st.text_input("Enter username")
def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')

# Initialize results  
if "results" not in st.session_state:
    st.session_state.results = []
# Execute SQL query
def execute_sql(sql):
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql(sql, conn)
        return df
    except Exception as e:
        print(e)
        return "ERROR: "+ str(e)
st.sidebar.header('Interaction Log')


download_dict = {} 

# Display log function
def display_log():

  for result in st.session_state.results:
    
    if isinstance(result['df'], pd.DataFrame):
        
      # Generate unique name
      name = f"result_{len(download_dict)+1}"
      
      # Add dataframe to dictionary
      download_dict[name] = result['df']

      st.sidebar.code(result['sql'])
      st.sidebar.dataframe(result['df'])

      # Download button
      csv = convert_df(result['df'])
      st.sidebar.download_button(
        label=f"Download {name}",
        data=csv, 
        file_name=f"{name}.csv",
        mime='text/csv'
      )      
display_log()




if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    if message["role"] != "system":
        with st.chat_message(message["role"]):
          st.markdown(message["content"])




# append the context to messages list for better response
st.session_state.messages.append({"role": "system", "content": context})

# set the model (#gpt-3.5-turbo original)
if "openai_model" not in st.session_state:
    st.session_state["openai_model"] = "gpt-3.5-turbo-16k" 

if username:
  if username not in allowed_users:
     st.error('Invalid username')
  else:
     if prompt := st.chat_input("Enter your query here.."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user",avatar="💻"):
            st.markdown(prompt)

        with st.chat_message("assistant",avatar="🤖"):
            message_placeholder = st.empty()
            full_response = ""
            sql_query = ""
            try:
                for response in openai.ChatCompletion.create(
                    model=st.session_state["openai_model"],
                    messages=[
                        {"role": m["role"], "content": m["content"]}
                        for m in st.session_state.messages
                    ],
                    temperature = 0,
                    stream=True,
                ):   
                    
                    sql_query += response.choices[0].delta.get("content", "")

                            
                    full_response += response.choices[0].delta.get("content", "")
                    message_placeholder.markdown(full_response + "▌")
                message_placeholder.markdown(full_response)
                log_to_sheet(username, prompt, full_response)
                
                try:
                    results = execute_sql(sql_query)
                    
                    if results is not None or results.startswith("ERROR"):
                        df = results
                    # Append latest result
                        latest_result = {'sql': sql_query, 'df': df}
                        st.session_state.results.append(latest_result)

                        #st.code(sql_query, language='sql')
                        st.dataframe(df)
                        csv = convert_df(df)
                        st.download_button(
                        "Press to Download",
                        csv,
                        "file.csv",
                        "text/csv",
                        key='download-csv'
                        )
                        
                        st.sidebar.empty()
                        # Display latest in log
                        # Show latest result in sidebar
                        st.sidebar.code(latest_result['sql'], language='sql')
                        st.sidebar.dataframe(latest_result['df'])

                                    
                except Exception as e:
                      st.write("Please Enter a valid query related to the database or try to be more specific")

            except openai.error.InvalidRequestError as e:
                    # Check if error is due to context length
                    if "maximum context length" in str(e):
                        st.warning("Sorry, I have reached my maximum conversational context length. Let's start a new conversation! Do so by refreshing the Page")
                    else:
                        raise e
            except Exception as e:
                    st.error(f"Oops, an error occurred: {type(e).__name__}, args: {e.args}") 
                            
            
        st.session_state.messages.append({"role": "assistant", "content": full_response})


