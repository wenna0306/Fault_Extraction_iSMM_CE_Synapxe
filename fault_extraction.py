import pandas as pd
import numpy as np
import datetime
import requests
import os
import pytz
import ast
from supabase import create_client
from dotenv import load_dotenv

# Define Singapore timezone
sg_timezone = pytz.timezone('Asia/Singapore')
today = datetime.datetime.now(sg_timezone).date()

from dotenv import load_dotenv
load_dotenv(dotenv_path=r"api_key.env")
# Fetch from environment variables
email = os.getenv("email")
password = os.getenv("password")

site_name = 113


# Function to get access token
def get_access_token(email, password):
    url = "https://ismm.sg/ce/api/auth/login?"
    params = {'email': email, 'password': password}
    response = requests.post(url, params=params)
    return response.json().get('access_token') if response.status_code == 200 else None

# Step 1: Get the access token
access_token = get_access_token(email, password)

# Function to fetch paginated fault data within the date range
def fetch_faults(access_token):
    start_date = today - datetime.timedelta(days=90)  # 3 months before today
    # Format dates as YYYY-MM-DD
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = today.strftime("%Y-%m-%d")

    # Status filters
    statuses = []

    # Pagination variables
    per_page = 10  # Adjust based on API limits
    total_pages = 5000  # Define how many pages you want to fetch (e.g., 5 pages)

    all_data = []  # List to collect all the fault data

    # Loop through pages and fetch data
    for page in range(1, total_pages + 1):
        # Construct the URL with parameters directly in the query string
        url = f"https://ismm.sg/ce/api/faults?site={site_name}&start_date={start_date_str}&end_date={end_date_str}&status={','.join(statuses)}&page={page}&per_page={per_page}"

        # Make GET request
        response = requests.get(url, headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"})

        if response.status_code == 200:
            data = response.json()
            if not data:  # Stop if no data returned
                print("No more data.")
                break

            # Collect the data
            all_data.extend(data['data'])  # Extend the all_data list with the 'data' part of the response

            # Check if fewer results than per_page (no more pages to fetch)
            if len(data['data']) < per_page:
                print("No more pages.")
                break
        else:
            print(f"Error fetching data: {response.status_code} - {response.json()}")
            break

    return all_data  # Return the collected data


# Step 2: Fetch fault data within the date range and status filters
if access_token:
    all_fault_data = fetch_faults(access_token)  # Get the fault data

    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(all_fault_data)

df = df.loc[:, ["fault_number", "site_fault_number", "trade_name", "category_name","type_name", "impact_name", "site_and_location", 
                "created_user","responded_date", "site_visited_date", "ra_acknowledged_date", "work_started_date", "work_completed_date", 
                "latest_status", "fault_remarks", 'source', "created_at"]]

# Convert the string to a Python dictionary
df["site_and_location"] = df["site_and_location"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

# Extract the first dictionary from the 'data' list
df["site_and_location"] = df["site_and_location"].apply(lambda x: x["data"][0] if isinstance(x, dict) and "data" in x and isinstance(x["data"], list) else None)


# Flatten the extracted dictionary
df_loc = pd.json_normalize(df["site_and_location"])
df_loc = df_loc.loc[:, ["site_name", "building_name", "floor_name", "room_name", "assets"]]
df_loc = df_loc.set_index(df["site_and_location"].index)


# Merge with original DataFrame
df_flattened = df.drop(columns=["site_and_location"]).join(df_loc)

df_flattened.columns = ['fault_number', 'Site Fault Number', 'Trade', 'Trade Category', 'Type of Fault',
       'impact', 'Reported By', 'Fault Acknowledged Date', 'Responded on Site Date',
       'RA Conducted Date', 'Work Started Date', 'Work Completed Date',
       'Status', 'Remarks', 'Source', 'Reported Date', 'Site',
       'Building', 'Floor', 'Room', 'Assets']

df_final = df_flattened[['fault_number', 'Site Fault Number', 'Trade', 'Trade Category', 'Type of Fault',
                             'impact', 'Site', 'Building', 'Floor', 'Room', 'Assets', 'Reported Date',
                             'Fault Acknowledged Date', 'Responded on Site Date','RA Conducted Date',
                             'Work Started Date', 'Work Completed Date', 'Status', 'Reported By', 'Remarks', 'Source']]

df_final["Fault Link"] = "https://ismm.sg/ce/fault/" + df_final['fault_number'].str.replace('FID', '', regex=False)

# Load environment variables
load_dotenv(dotenv_path="myenv.env")
# Fetch from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Create Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Convert DataFrame to list of dicts
data_dic = df_final.to_dict(orient="records")

# Upsert data into Supabase table
supabase.table("fault_AFT").upsert(data_dic, on_conflict=["fault_number"]).execute()
