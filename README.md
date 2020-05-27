# EMT URBAN BUSES 

# Data used by the server 
All the data used by the server can be found in this link: https://mega.nz/folder/QRIGnQRZ#7fJVQcapLkSp7jGGz0WZeQ

## Raw data
Use the data in the link above or collect it yourself with the script: **/Scripts/CollectData/retrieve_data.py** following the steps used to collect the real time data indicated below.

## Processed data:
Download from the link above the files : 
* **buses_data_pc.csv** - Preprocessed and cleaned raw data
* **time_bt_stops.csv** - Times between stops
* **headways.csv** - Headways between buses
And place the inside **/Data/Processed/**

Or process them yourself with the scripts inside **/Scripts/ProcessData/** from the raw data **/Data/Raw/buses_data.csv**

## Real time data :
* **1.** Create an account at : https://mobilitylabs.emtmadrid.es/ 
* **2.** Create a file called api_credentials.py with your credentials
* **3.** Run the script **/Scripts/CollectDataretrieve_data.py** inside **** to start collecting real time data

# Start the server 

## Steps to use the server
* **1.** Install the packages inside **requirements.txt**
* **2.** Run the server using the script **run_server.sh**
* **3.** Enter the server with direction : **0.0.0.0:8050**



