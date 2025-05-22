from API_script import Thema_data_API
import requests
import pandas as pd
import os
from getpass import getpass

# specify username and password
username = "yourEmail@company.com"
password = getpass()

# specify a folder for the output
output_folder = "Thema_API_output/Market_outlook/"
os.makedirs(output_folder, exist_ok=True)

# initiates an API object of type Thema_data_API
API_object = Thema_data_API(username=username, password=password)

# example of fetching master data
master_data = API_object.get_master_data()

# create master data excel file
with pd.ExcelWriter(f"{output_folder}Master_data.xlsx", engine="xlsxwriter") as writer:
    for name, df in master_data.items():
        df.to_excel(writer, sheet_name=str(name), index=False)

# Hourly Data input example
# can specify multiple values per parameter by encapsulating in {}. Script will fetch all valid combinations
# parameters that are not specified, or given as None, will be filled with all valid inputs by program. 
# Note that this might lead to high memory usage and long execution time
json = {
        "scenario": "Base",
        "region": "Nordics",
        "edition": None,
        "country": {"Norway", "Sweden"},
        "zone": {"NO2", "NO1", "SE2"}
        }

# example of calling the hourly data API and writing the results to excel
hourly_data = API_object.get_hourly_data(json)
hourly_data.to_excel(f"{output_folder}Hourly_data.xlsx", index=False)

# Annual Data input example
# can specify multiple values per parameter by encapsulating in {}. Script will fetch all valid combinations
# parameters that are not specified, or given as None, will be filled with all valid inputs by program. 
# Note that this might lead to high memory usage and long execution time
json = {
    "scenario": {"Base", "Turbulent transition", "Technotopia"},
    "group": {"Real prices", "Generation"},
    "indicator": {"Gas price", "Coal price", "Nuclear"},
    "region": "Nordics",
    "edition": "September 2022",
    "country": {"Norway", "Sweden"},
    "zone": {"NO1", "NO2", "SE1", "SE2", "SE3", "SE4"}
        }

# example of calling the annual data API and writing the results to excel
annual_data = API_object.get_annual_data(json)
annual_data.to_excel(f"{output_folder}Annual_data.xlsx", index=False)

# Monthly Data input example
# can specify multiple values per parameter by encapsulating in {}. Script will fetch all valid combinations
# parameters that are not specified, or given as None, will be filled with all valid inputs by program. 
# Note that this might lead to high memory usage and long execution time
json = {
    "scenario": "Base",
    "group": "Real prices",
    "indicator": "Base price",
    "region": "Nordics",
    "edition": None, 
    "country": "Denmark",
    "zone": "DK2"
    }

#example of calling the monthly data API and writing the results to excel
monthly_data = API_object.get_monthly_data(json)
monthly_data.to_excel(f"{output_folder}Monthly_data.xlsx", index=False)

# if specifying multiple values per parameter, this gives overview of rejected values combinations
# non-rejected combinations are still included in Annual and Hourly output
rejected_combinations = API_object.get_rejected_combinations()
if not rejected_combinations.empty:
    rejected_combinations.to_excel(f"{output_folder}Rejected_combinations.xlsx", index=False)