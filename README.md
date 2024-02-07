# thema-market-outlook-api

API_script.py provides classes to simplify connecting to the Thema Data API.  
Technical documentation of the Thema Data API can be found here: https://portal.thema.no/customer-api/swagger-ui/index.html

The file has has three classes:

1. Thema_data_API
A class to get market outlook data.
This class has three public functions:

get_master_data()  
Returns the combinations of input parameters avaliable to individual users.  
This is used as user input into the functions that fetches annual and hourly data.  
  
get_hourly_data()  
Returns hourly data for the specified combination of scenario, region, edition, country and zone.  

get_annual_data()  
Returns annual data for the specified combination of scenario, group, region, edition, indicator, country and zone.  

Example code to interact with the script can be found in Market_outlook_data_example_code.py


2. Thema_technology_data_API
A class to get technology outlook data
This class has two public functions:

get_master_data()  
Returns the combinations of input parameters avaliable to individual users.  
This is used as user input into the functions that fetches annual data. 

get_annual_data()
Returns annual data for the specified combination of scenario, country, edition, indicator, technology and category.

Example code to to interact with the script can be found in Technology_outlook_data_example_code.py. 


2. Thema_hydrogen_data_API
A class to get hydrogen outlook data
This class has two public functions:

get_master_data()  
Returns the combinations of input parameters avaliable to individual users.  
This is used as user input into the functions that fetches annual data. 

get_annual_data()
Returns annual data for the specified combination of scenario, country, edition, group and indicator.

Example code to to interact with the script can be found in Hydrogen_outlook_data_example_code.py.



