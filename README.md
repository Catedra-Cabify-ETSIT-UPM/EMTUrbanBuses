# EMT URBAN BUSES 

# Data used by the server 

## Raw data
Download the **buses_data.csv** file with the raw collected data from this link and place it inside /Data/Raw

## Processed data:
Download the files : 
**buses_data_pc** Preprocessed and cleaned raw data
**time_bt_stops.csv** Times between stops
**headways.csv** Headways between buses
Or process them yourself with the scripts inside /Scripts/ProcessData

## Real time data :
* **1.** Create an account at : https://mobilitylabs.emtmadrid.es/ 
* **2.** Create a file called api_credentials.py with your credentials
* **3.** Run the script **retrieve_data.py** inside **/Scripts/CollectData** to start collecting real time data

# Start the server 

## Steps to use the server
* **1.** Install the packages inside **requirements.txt**
* **2.** Run the server using the script **run_server.sh**
* **3.** Enter the server with direction : **0.0.0.0:8050**



