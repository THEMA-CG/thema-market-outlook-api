# thema-market-outlook-api

API_script.py provides a class to simplify connecting to the Thema market outlook API.  
The class Thema_API has three public functions:

1. get_master_data()  
Returns the combinations of input parameters avaliable to individual users.  
This is used as user input into the functions that fetches annual and hourly data.  
  
2. get_hourly_data()  
Returns hourly data for the specified combination of scenario, region, edition, country and zone.  

3. get_annual_data()  
Returns annual data for the specified combination of scenario, group, region and edition.  
The parameters indicator, country and zones are optional.  
  
The end of the script contains example code for using the class.  
Remember to update username and password in the example code before running it. 
The script has some built in validation and error handling to simplify use of the API.  
Technical documentation of the API can be found here: https://portal.thema.no/customer-api/swagger-ui/index.html
