import pandas as pd
import geopandas as gpd
import json
import re
import os.path
import random

import time
import datetime
from datetime import timedelta
from threading import Timer

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import asyncio
from concurrent.futures import ThreadPoolExecutor

from shapely.geometry import Point, LineString

# WE LOAD THE DATA
stops = gpd.read_file('M6Data/stops.json')
route_lines = gpd.read_file('M6Data/route_lines.json')
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

#Get bus estimated coords
def point_by_distance_on_line (line, line_lenght, distance, origin_point) :
    """
    Returns the coordinates of the bus location

        Parameters
        ----------
        line : LineString
            The line that the bus belongs to
        line_length : float
            The length of the line
        distance : float
            The distance of the bus to the stop in kilometers
        origin_point : Point
            The location of the bus stop
    """
    
    #First we calculate the normalized distance of the bus from the start of the line
    #by substracting the distance of the bus to the stop to the distance of the stop to the start of the line
    #which is returned by the project method of the shapely module
    normalized_distance = line.project(origin_point,normalized=True) - distance/line_lenght
    
    #Then we get the the coordinates of the point that is at the normalized distance obtained 
    #before from the start of the line with the interpolate method
    interpolated_point = line.interpolate(normalized_distance,normalized=True)
    
    #And we return the coordinates of the point
    return (interpolated_point.x,interpolated_point.y)

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
        lineId : string
            The line id
        stopId : string
            The stop code
        accessToken: string
            The accessToken obtained in the login
    """
    
    #We build the body for the request
    body = {
        'cultureInfo': 'EN',
        'Text_StopRequired_YN': 'Y',
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
    for lineId in requested_lines :
        if line_stops_dict[lineId] != None :
            if line_stops_dict[lineId]['1'] != None :
                stops_of_lines = stops_of_lines + line_stops_dict[lineId]['1']['stops']
            if line_stops_dict[lineId]['2'] != None :
                stops_of_lines = stops_of_lines + line_stops_dict[lineId]['2']['stops']

    stops_of_lines = list(set(stops_of_lines))
     
    #Prepair the lineIds for the request
    if requested_lines == ['1','82','91','92','99','132'] :
        requested_lines_alt = ['1','82','F','G','U','132']
    elif requested_lines == ['1','82','132'] :
        requested_lines_alt = ['1','82','132']
    elif requested_lines == ['502','506'] :
        requested_lines_alt = ['N2','N6']
        
    #We get the selected lines from the dataframe
    lines_selected = route_lines.loc[route_lines['line_id'].isin(requested_lines)]
    
    #Get the line rows
    line1_dir1 = lines_selected.loc[(route_lines['line_id']=='1')&(route_lines['direction']=='1')]
    line1_dir2 = lines_selected.loc[(route_lines['line_id']=='1')&(route_lines['direction']=='2')]
        
    line82_dir1 = lines_selected.loc[(route_lines['line_id']=='82')&(route_lines['direction']=='1')]
    line82_dir2 = lines_selected.loc[(route_lines['line_id']=='82')&(route_lines['direction']=='2')]
        
    line91_dir1 = lines_selected.loc[(route_lines['line_id']=='91')&(route_lines['direction']=='1')]
    line91_dir2 = lines_selected.loc[(route_lines['line_id']=='91')&(route_lines['direction']=='2')]
        
    line92_dir1 = lines_selected.loc[(route_lines['line_id']=='92')&(route_lines['direction']=='1')]
    line92_dir2 = lines_selected.loc[(route_lines['line_id']=='92')&(route_lines['direction']=='2')]
        
    line99_dir1 = lines_selected.loc[(route_lines['line_id']=='99')&(route_lines['direction']=='1')]
    line99_dir2 = lines_selected.loc[(route_lines['line_id']=='99')&(route_lines['direction']=='2')]
        
    line132_dir1 = lines_selected.loc[(route_lines['line_id']=='132')&(route_lines['direction']=='1')]
    line132_dir2 = lines_selected.loc[(route_lines['line_id']=='132')&(route_lines['direction']=='2')]
        
    line502_dir1 = lines_selected.loc[(route_lines['line_id']=='502')&(route_lines['direction']=='1')]
    line502_dir2 = lines_selected.loc[(route_lines['line_id']=='502')&(route_lines['direction']=='2')]
        
    line506_dir1 = lines_selected.loc[(route_lines['line_id']=='506')&(route_lines['direction']=='1')]
    line506_dir2 = lines_selected.loc[(route_lines['line_id']=='506')&(route_lines['direction']=='2')]
        
    #The keys for the dataframe that is going to be built
    keys = ['bus','line','stop','datetime','isHead','destination','deviation','estimateArrive','DistanceBus','request_time']
    
    #Function to perform the requests asynchronously, performing them concurrently would be too slow
    async def get_data_asynchronous() :
        global account_index
        global accessToken
        global out_of_hits
        
        row_list = []
        points_list = []
        real_coords_list = []
        
        #Información de la recogida de datos
        n_ok_answers = 0
        n_not_ok_answers = 0
        
        #We set the number of workers that is going to take care about the requests
        with ThreadPoolExecutor(max_workers=30) as executor:
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
                                return [row_list,points_list,real_coords_list,n_ok_answers,n_not_ok_answers]
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
                stop_coords = Point(arrival_data['data'][0]['StopInfo'][0]['geometry']['coordinates'])
                
                #We get the buses data 
                buses_data = arrival_data['data'][0]['Arrive']
                
                for bus in buses_data :
                    if bus['line'] in requested_lines_alt :
                        #Select the line for the calc
                        if bus['line'] == '1' :
                            line_dir1 = line1_dir1
                            line_dir2 = line1_dir2
                        elif bus['line'] == '82' :
                            line_dir1 = line82_dir1
                            line_dir2 = line82_dir2
                        elif bus['line'] == 'F' :
                            line_dir1 = line91_dir1
                            line_dir2 = line91_dir2
                        elif bus['line'] == 'G' :
                            line_dir1 = line92_dir1
                            line_dir2 = line92_dir2
                        elif bus['line'] == 'U' :
                            line_dir1 = line99_dir1
                            line_dir2 = line99_dir2
                        elif bus['line'] == '132' :
                            line_dir1 = line132_dir1
                            line_dir2 = line132_dir2
                        elif bus['line'] == 'N2' :
                            line_dir1 = line502_dir1
                            line_dir2 = line502_dir2
                        elif bus['line'] == 'N6' :
                            line_dir1 = line506_dir1
                            line_dir2 = line506_dir2
                        
                        #Real coordinates provided by the API
                        real_coords_list.append(Point(bus['geometry']['coordinates']))
                        #We calculate the bus position depending on the direction it belongs to
                        dests_1 = ['HOSPITAL LA PAZ','CIUDAD UNIVERSITARIA','PARANINFO','PITIS','PROSPERIDAD','VALDEBEBAS','LAS ROSAS']
                        if bus['destination'] in dests_1 :
                            points_list.append(Point(point_by_distance_on_line(line_dir1['geometry'],line_dir1['dist'],bus['DistanceBus']/1000,stop_coords)))
                        else :
                            points_list.append(Point(point_by_distance_on_line(line_dir2['geometry'],line_dir2['dist'],bus['DistanceBus']/1000,stop_coords)))
                        bus['datetime'] = date_time
                        bus['request_time'] = lapsed_time
                        values = [bus[key] for key in keys]
                        row_list.append(dict(zip(keys, values)))
                        
                    
        return [row_list,points_list,real_coords_list,n_ok_answers,n_not_ok_answers]
    
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
        points_list = future_result[1]
        real_coords_list = future_result[2]
        n_ok_answers = future_result[3]
        n_not_ok_answers = future_result[4]
        
        #We create the dataframe of the buses
        buses_df = pd.DataFrame(row_list, columns=keys)
        final_coords_lat_list = []
        final_coords_lon_list = []
        real_coords = []
        #Use the real coords if they exist
        for i in range(0,len(points_list)) :
            if (real_coords_list[i].y!=0)&(real_coords_list[i].x!=0) :
                final_coords_lat_list.append(real_coords_list[i].y)
                final_coords_lon_list.append(real_coords_list[i].x)
                real_coords.append(1)
            else :
                final_coords_lat_list.append(points_list[i].y)
                final_coords_lon_list.append(points_list[i].x)
                real_coords.append(0)
        #We add the coord columns to the dataframe
        buses_df['real_coords'] = real_coords
        buses_df['lat'] = final_coords_lat_list
        buses_df['lon'] = final_coords_lon_list
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
                    print('Retrieve data from lines 1,82,91,92,99,132 - 207 Stops - {}\n'.format(datetime.datetime.now()))
                    requested_lines = ['1','82','91','92','99','132']
                    rt = RepeatedTimer(55, get_arrival_data, requested_lines)
                    rt_started = True
            else :
                #Stop timer if it exists
                if rt_started :
                    print('Stop retrieving data from lines 1,82,91,92,99,132 - 207 Stops - {}\n'.format(datetime.datetime.now()))
                    rt.stop()
                    rt_started = False
        #If we are in Saturday or Sunday
        else : 
            #Retrieve data from lines 69,82,132 - 185 Stops
            if time_in_range(start_time_day,end_time_day,now.time()) :
                if not rt_started :
                    print('Retrieve data from lines 69,82,132 - 185 Stops - {}\n'.format(datetime.datetime.now()))
                    requested_lines = ['1','82','132']
                    rt = RepeatedTimer(55, get_arrival_data, requested_lines)
                    rt_started = True
            #Retrieve data from lines 502,506 - 131 Stops
            elif time_in_range(start_time_night,end_time_night,now.time()) :
                if not rt_started :
                    print('Retrieve data from lines 502,506 - 131 Stops - {}\n'.format(datetime.datetime.now()))
                    requested_lines = ['502','506']
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