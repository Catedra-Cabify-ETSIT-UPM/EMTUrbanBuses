import pandas as pd
import json

import re
import os.path
import random
import math

import time
import datetime
from datetime import timedelta
from threading import Timer

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import asyncio
from concurrent.futures import ThreadPoolExecutor

# WE LOAD THE DATA
stops = pd.read_csv('M6Data/stops.csv')
lines_shapes = pd.read_csv('M6Data/lines_shapes.csv')
with open('M6Data/line_stops_dict.json', 'r') as f:
    line_stops_dict = json.load(f)

class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer     = None
        self.interval   = interval
        self.function   = function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False

# FUNCTIONS
def find_nearest_row(df,lat,lon) :
    """
    Returns the row nearest to the coordinates passed in the dataframe

        Parameters
        ----------
        df : dataframe
            Dataframe where we want to find the row
        lat: float
        lon: float
    """
    min_dist_error = 1000000.0
    for row in df.itertuples() :
        error = math.sqrt(abs(row.lat-lat)+abs(row.lon-lon))
        if  error < min_dist_error :
            min_dist_error = error
            nearest_row = row
    return nearest_row

def find_nearest_row_by_dist(df,dist_traveled) :
    """
    Returns the row nearest to the distance traveled passed in the dataframe

        Parameters
        ----------
        df : dataframe
            Dataframe where we want to find the row
        dist_traveled : float
    """
    min_dist_error = 1000000.0
    for row in df.itertuples() :
        error = abs(row.dist_traveled-dist_traveled)
        if  error < min_dist_error :
            min_dist_error = error
            nearest_row = row
    return nearest_row

def point_by_distance_on_line (line,distance,stop_lat,stop_lon) :
    """
    Returns the coordinates of the bus location

        Parameters
        ----------
        line: DataFrame
            Points belonging to the requested line
        distance : float
            Distance of the bus to the stop in meters
        stop_lat : float
        stop_lon : float
    """

    nearest_row_to_stop = find_nearest_row(line,stop_lat,stop_lon)
    dist_traveled_of_bus = nearest_row_to_stop.dist_traveled - distance
    nearest_row_to_distance = find_nearest_row_by_dist(line,dist_traveled_of_bus)

    #And we return the coordinates of the point
    return nearest_row_to_distance.lon,nearest_row_to_distance.lat

#Check if time is inside range
def time_in_range(start, end, x):
    """Return true if x is in the range [start, end]"""
    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end

# API FUNCTIONS
def requests_retry_session(retries=3,backoff_factor=0.3,status_forcelist=(500, 502, 504),session=None):
    '''
    Function to ensure we get a good response for the request
    '''
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_access_token(email,password) :
    '''
    Returns the access token of the EMT Madrid API

        Parameters
        ----------
        email : string
            The email of the account
        password : string
            Password of the account
    '''
    try:
        if account_index == 0 :
            #Special request for the account with API
            response = requests_retry_session().get(
                'https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/',
                headers={
                    'X-ClientId':XClientId,
                    'passKey':passKey
                },
                timeout=5
            )
        else :
            response = requests_retry_session().get(
                'https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/',
                headers={
                    'email':email,
                    'password':password
                },
                timeout=5
            )
        json_response = response.json()
        return json_response
    except requests.exceptions.RequestException as e:
        print(e + '\n')
        return 'Error'

def get_arrival_times(stopId,accessToken) :
    """
    Returns the arrival data of buses for the desired stop and line

        Parameters
        ----------
        stopId : string
            The stop code
        accessToken: string
            The accessToken obtained in the login
    """

    #We build the body for the request
    body = {
        'cultureInfo': 'EN',
        'Text_StopRequired_YN': 'N',
        'Text_EstimationsRequired_YN': 'Y',
        'Text_IncidencesRequired_YN': 'N',
        'DateTime_Referenced_Incidencies_YYYYMMDD':'20200130'
    }

    #And we perform the request
    try:
        response = requests_retry_session().post(
            'https://openapi.emtmadrid.es/v2/transport/busemtmad/stops/{}/arrives/{}/'.format(stopId,''),
            data = json.dumps(body),
            headers = {
                'accessToken': accessToken,
                'Content-Type': 'application/json'
            },
            timeout = 20
        )
        #Return the response if we received it ok
        return response
    except requests.exceptions.RequestException as e:
        print(e + '\n')
        return 'Error'

def get_arrival_data(requested_lines) :
    """
    Returns the data of all the buses inside the requested lines

        Parameters
        ----------
        requested_lines : list
            List with the desired line ids
    """

    #We get the list of stops to ask for
    stops_of_lines = []
    for line_id in requested_lines :
        line_id = str(line_id)
        if line_stops_dict[line_id] != None :
            if line_stops_dict[line_id]['1'] != None :
                stops_of_lines = stops_of_lines + line_stops_dict[line_id]['1']
            if line_stops_dict[line_id]['2'] != None :
                stops_of_lines = stops_of_lines + line_stops_dict[line_id]['2']

    #List of different stops
    stops_of_lines = list(set(stops_of_lines))

    #The keys for the dataframe that is going to be built
    keys = ['bus','line','stop','datetime','isHead','destination','deviation','estimateArrive','DistanceBus','request_time']

    #Function to perform the requests asynchronously, performing them concurrently would be too slow
    async def get_data_asynchronous() :
        global account_index
        global accessToken
        global out_of_hits

        row_list = []
        calc_lats,calc_lons = [],[]
        given_lats,given_lons = [],[]

        #Información de la recogida de datos
        n_ok_answers = 0
        n_not_ok_answers = 0

        #We set the number of workers that is going to take care about the requests
        with ThreadPoolExecutor(max_workers=50) as executor:
            #We create a loop object
            loop = asyncio.get_event_loop()
            #And a list of tasks to be performed by the loop
            tasks = [
                loop.run_in_executor(
                    executor,
                    get_arrival_times, #Function that is gonna be called by the tasks
                    *(stopId,accessToken)  #Parameters for the function
                )
                for stopId in stops_of_lines
            ]
            #We randomize the order of the tasks
            random.shuffle(tasks)
            #And finally we perform the tasks and gather the information returned by them into two lists
            for response in await asyncio.gather(*tasks) :
                if not response == 'Error' :
                    if response.status_code == 200 :
                        arrival_data = response.json()
                        n_ok_answers = n_ok_answers + 1
                        if arrival_data['code'] == '98':
                            #If we spend all the hits we switch the account and wait for next request
                            out_of_hits = True
                            print('Hits of account_index = {} spent - {}\n'.format(account_index,datetime.datetime.now()))
                            #Return the data gathered if the request that fails isnt the first
                            if len(row_list) == 0 :
                                return None
                            else :
                                print('Appending data gathered before end of hits\n')
                                return row_list, calc_lats, calc_lons, given_lats, given_lons, n_ok_answers, n_not_ok_answers
                    else :
                        #If the response isnt okey we pass to the next iteration
                        n_not_ok_answers = n_not_ok_answers + 1
                        continue
                else :
                    #If the response isnt okey we pass to the next iteration
                    n_not_ok_answers = n_not_ok_answers + 1
                    continue

                lapsed_time = int(re.search('lapsed: (.*) millsecs', arrival_data['description']).group(1))
                date_time = datetime.datetime.strptime(arrival_data['datetime'], '%Y-%m-%dT%H:%M:%S.%f')

                #We get the buses data
                buses_data = arrival_data['data'][0]['Arrive']
                stop = stops.loc[stops.id == int(buses_data[0]['stop'])]
                for bus in buses_data :
                    #Get the line rows for each direction
                    line1 = lines_shapes.loc[(lines_shapes.line_sn==bus['line'])&(lines_shapes.direction==1)]
                    line2 = lines_shapes.loc[(lines_shapes.line_sn==bus['line'])&(lines_shapes.direction==2)]

                    if line1.iloc[0].line_id in requested_lines :
                        #Given coordinates provided by the API
                        given_lons.append(bus['geometry']['coordinates'][0])
                        given_lats.append(bus['geometry']['coordinates'][1])

                        #We calculate the bus position depending on the direction it belongs to
                        dests_1 = ['HOSPITAL LA PAZ','CIUDAD UNIVERSITARIA','PARANINFO','PITIS','PROSPERIDAD','VALDEBEBAS','LAS ROSAS']
                        if bus['destination'] in dests_1 :
                            calc_lon,calc_lat = point_by_distance_on_line(line1,bus['DistanceBus'],stop.lat,stop.lon)
                            calc_lons.append(calc_lon)
                            calc_lats.append(calc_lat)
                        else :
                            calc_lon,calc_lat = point_by_distance_on_line(line2,bus['DistanceBus'],stop.lat,stop.lon)
                            calc_lons.append(calc_lon)
                            calc_lats.append(calc_lat)
                        bus['datetime'] = date_time
                        bus['request_time'] = lapsed_time
                        values = [bus[key] for key in keys]
                        row_list.append(dict(zip(keys, values)))


        return row_list, calc_lats, calc_lons, given_lats, given_lons,n_ok_answers,n_not_ok_answers

    #We declare the loop and call it, then we run it until it is complete
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(get_data_asynchronous())
    loop.run_until_complete(future)

    #And once it is completed we gather the information returned by it like this
    future_result = future.result()
    if future_result == None :
        return None
    else :
        row_list = future_result[0]
        calc_lats = future_result[1]
        calc_lons = future_result[2]
        given_lats = future_result[3]
        given_lons = future_result[4]
        n_ok_answers = future_result[5]
        n_not_ok_answers = future_result[6]

        #We create the dataframe of the buses
        buses_df = pd.DataFrame(row_list, columns=keys)
        final_lats,final_lons = [],[]
        given_coords = []
        #Use the given coords if they exist
        for i in range(len(calc_lats)) :
            if (given_lats[i]!=0)&(given_lons[i]!=0) :
                final_lats.append(given_lats[i])
                final_lons.append(given_lons[i])
                given_coords.append(1)
            else :
                final_lats.append(calc_lats[i])
                final_lons.append(calc_lons[i])
                given_coords.append(0)

        #We add the coord columns to the dataframe
        buses_df['given_coords'] = given_coords
        buses_df['lat'] = final_lats
        buses_df['lon'] = final_lons

        #And we append the data to the csv
        f='buses_data.csv'
        if os.path.isfile(f) :
            buses_df.to_csv(f, mode='a', header=False)
        else :
            buses_df.to_csv(f, mode='a', header=True)

        print('There were {} ok responses and {} not okey responses - {}'.format(n_ok_answers,n_not_ok_answers,datetime.datetime.now()))
        print('{} new rows appended to {}\n'.format(buses_df.shape[0],f))


#Global vars
from api_credentials import emails,passwords,XClientId,passKey
account_index = 0
accessToken = ''
out_of_hits = True

def main():
    global account_index
    global accessToken
    global out_of_hits

    rt_started = False

    #Initial value of accessToken
    for i in range(0,5) :
        account_index = i

        #Try until we get response to the login without errors
        json_response = 'Error'
        while json_response == 'Error' :
            json_response = get_access_token(emails[account_index],passwords[account_index])

        if json_response['code'] == '98' :
            print('Account {} has no hits available\n'.format(account_index))
            if i == 4 :
                print('None of the accounts has hits available\n')
                i = 0
            else :
                pass
        elif (json_response['code'] == '00') | (json_response['code'] == '01') :
            accessToken = json_response['data'][0]['accessToken']
            print('Making requests from account {} - accessToken : {}\n'.format(account_index,accessToken))
            out_of_hits = False
            break

    #Normal buses hours range
    start_time_day = datetime.time(7,0,0)
    end_time_day = datetime.time(23,0,0)
    #Night buses hours range
    start_time_night = datetime.time(0,0,0)
    end_time_night = datetime.time(5,30,0)

    while True :
        #Retrieve data every interval seconds if we are between 6:00 and 23:30
        now = datetime.datetime.now()

        #If we have lost all the hits
        if out_of_hits :
            #Wait until we have hits available again
            if ( (now.weekday() in [0,1,2,3,4]) & (time_in_range(start_time_day,end_time_day,now.time())) ) | (now.weekday() in [5,6]) :
                for i in range(0,5) :
                    account_index = i

                    #Try until we get response to the login without errors
                    json_response = 'Error'
                    while json_response == 'Error' :
                        json_response = get_access_token(emails[account_index],passwords[account_index])

                    if json_response['code'] == '98' :
                        print('Account {} has no hits available\n'.format(account_index))
                        if i == 4 :
                            print('None of the accounts has hits available - Sleeping 10 minutes - {}\n'.format(datetime.datetime.now()))
                            i = 0
                            if rt_started :
                                print('Stop retrieving data - {}'.format(datetime.datetime.now()))
                                rt.stop()
                                rt_started = False
                            time.sleep(600)
                        else :
                            pass
                    elif (json_response['code'] == '00') | (json_response['code'] == '01') :
                        accessToken = json_response['data'][0]['accessToken']
                        print('Making requests from account {} - accessToken : {}\n'.format(account_index,accessToken))
                        out_of_hits = False
                        break
            else :
                #We wait 10 minutes
                time.sleep(600)
            #We pass directly to next iteration
            continue

        #If we are not in the weekend
        if now.weekday() in [0,1,2,3,4] :
            #Retrieve data from lines 1,82,91,92,99,132 - 207 Stops
            if time_in_range(start_time_day,end_time_day,now.time()) :
                if not rt_started :
                    print('Retrieve data from lines 1,82,91(F),92(G),99(U),132 - 207 Stops - {}\n'.format(datetime.datetime.now()))
                    requested_lines = [1,82,91,92,99,132]
                    rt = RepeatedTimer(55, get_arrival_data, requested_lines)
                    rt_started = True
            else :
                #Stop timer if it exists
                if rt_started :
                    print('Stop retrieving data from lines 1,82,91(F),92(G),99(U),132  - 207 Stops - {}\n'.format(datetime.datetime.now()))
                    rt.stop()
                    rt_started = False
        #If we are in Saturday or Sunday
        else :
            #Retrieve data from lines 69,82,132 - 185 Stops
            if time_in_range(start_time_day,end_time_day,now.time()) :
                if not rt_started :
                    print('Retrieve data from lines 1,82,132 - 185 Stops - {}\n'.format(datetime.datetime.now()))
                    requested_lines = [1,82,132]
                    rt = RepeatedTimer(55, get_arrival_data, requested_lines)
                    rt_started = True
            #Retrieve data from lines 502,506 - 131 Stops
            elif time_in_range(start_time_night,end_time_night,now.time()) :
                if not rt_started :
                    print('Retrieve data from lines 502,506 - 131 Stops - {}\n'.format(datetime.datetime.now()))
                    requested_lines = [502,506]
                    rt = RepeatedTimer(70, get_arrival_data, requested_lines)
                    rt_started = True
            else :
                #Stop timer if it exists
                if rt_started :
                    print('Stop retrieving data for weekends - {}\n'.format(datetime.datetime.now()))
                    rt.stop()
                    rt_started = False

        #Wait 10 seconds till next loop (no need to run the loop faster)
        time.sleep(10)

if __name__== "__main__":
    main()
