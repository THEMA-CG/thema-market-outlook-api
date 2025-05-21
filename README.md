# thema-market-outlook-api

API_script.py provides classes to simplify connecting to the Thema Data API.  
Technical documentation of the Thema Data API can be found here: 
https://portal.thema.no/customer-api/swagger-ui/index.html

The file has has three classes:

# Thema_data_API
A class to get market outlook data.
This class has six public functions:

1. get_master_data()  
Returns the combinations of input parameters avaliable to individual users.  
This is used as user input into the functions that fetches annual and hourly data.  
  
2. get_hourly_data()  
Returns hourly data for the specified combination of scenario, region, edition, country and zone.  

3. get_annual_data()  
Returns annual data for the specified combination of scenario, group, region, edition, indicator, country and zone.

4. get_monthly_data()
Returns monthly data for the specified combination of scenario, group, region, edition, indicator, country and zone.

5. get_GO_data()
Returns Guarantees of Origin Outlook data for the specified combination of scenario, group, edition, indicator and zone.

6. get_PPA_data()
Returns Power Purchase Agreement data for the specified combination of scenario, group, edition and zone.

Example code to interact with the script can be found in Market_outlook_data_example_code.py


# Thema_technology_data_API
A class to get technology outlook data

This class has two public functions:

1. get_master_data()  
Returns the combinations of input parameters avaliable to individual users.  
This is used as user input into the functions that fetches annual data. 

2. get_annual_data()
Returns annual data for the specified combination of scenario, country, edition, indicator, technology and category.

Example code to to interact with the script can be found in Technology_outlook_data_example_code.py. 


# Thema_hydrogen_data_API
A class to get hydrogen outlook data

This class has two public functions:

1. get_master_data()  
Returns the combinations of input parameters avaliable to individual users.  
This is used as user input into the functions that fetches annual data. 

2. get_annual_data()
Returns annual data for the specified combination of scenario, country, edition, group and indicator.

Example code to to interact with the script can be found in Hydrogen_outlook_data_example_code.py.