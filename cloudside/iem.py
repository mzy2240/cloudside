# Author: Zeyu Mao (zeyumao2@tamu.edu)

from __future__ import print_function
import json
import time
import datetime
import pandas as pd
from io import StringIO
from typing import Union
from dateutil import parser
from collections import OrderedDict
import random
from rex import NSRDBX
import numpy as np
import pint 
import pint_pandas

# Python 2 and 3: alternative 4
# try:
#     from urllib.request import urlopen
# except ImportError:
#     from urllib2 import urlopen
from urllib.request import urlopen

# Number of attempts to download data
MAX_ATTEMPTS = 3
# HTTPS here can be problematic for installs that don't have Lets Encrypt CA
SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"


def download_data(uri):
    """Fetch the data from the IEM
    The IEM download service has some protections in place to keep the number
    of inbound requests in check.  This function implements an exponential
    backoff to keep individual downloads from erroring.
    Args:
      uri (string): URL to fetch
    Returns:
      string data
    """
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            obj = urlopen(uri, timeout=300)
            data = obj.read().decode("utf-8")
            if data is not None and not data.startswith("ERROR"):
                return data
        except Exception as exp:
            print("download_data(%s) failed with %s" % (uri, exp))
            time.sleep(5)
        attempt += 1

    print("Exhausted attempts to download, returning empty data")
    return ""


def get_stations_from_filelist(filename):
    """Build a listing of stations from a simple file listing the stations.
    The file should simply have one station per line.
    """
    stations = []
    for line in open(filename):
        stations.append(line.strip())
    return stations


def get_stations_from_networks(state):
    """Build a station list by using a bunch of IEM networks."""
    stations = []
    states = state
    networks = []
    for state in states.split():
        networks.append("%s_ASOS" % (state,))

    for network in networks:
        # Get metadata
        uri = (
            "https://mesonet.agron.iastate.edu/geojson/network/%s.geojson"
        ) % (network,)
        data = urlopen(uri)
        jdict = json.load(data)
        for site in jdict["features"]:
            stations.append(site["properties"]["sid"])
    return stations


def download_alldata():
    """An alternative method that fetches all available data.
    Service supports up to 24 hours worth of data at a time."""
    # timestamps in UTC to request data for
    startts = datetime.datetime(2012, 8, 1)
    endts = datetime.datetime(2012, 8, 2)
    interval = datetime.timedelta(hours=24)

    service = SERVICE + "data=all&tz=Etc/UTC&format=comma&latlon=yes&"

    now = startts
    while now < endts:
        thisurl = service
        thisurl += now.strftime("year1=%Y&month1=%m&day1=%d&")
        thisurl += (now + interval).strftime("year2=%Y&month2=%m&day2=%d&")
        print("Downloading: %s" % (now,))
        data = download_data(thisurl)
        outfn = "%s.txt" % (now.strftime("%Y%m%d"),)
        with open(outfn, "w") as fh:
            fh.write(data)
        now += interval


def main():
    """Our main method"""
    # timestamps in UTC to request data for
    startts = datetime.datetime(2012, 8, 1)
    endts = datetime.datetime(2012, 9, 1)

    # service = SERVICE + "data=all&tz=Etc/UTC&format=comma&latlon=yes&"
    service = SERVICE + "data=tmpc&data=drct&data=sped&tz=UTC&format=comma&latlon=yes&"

    service += startts.strftime("year1=%Y&month1=%m&day1=%d&")
    service += endts.strftime("year2=%Y&month2=%m&day2=%d&")

    # Two examples of how to specify a list of stations
    stations = get_stations_from_networks()
    # stations = get_stations_from_filelist("mystations.txt")
    for station in stations[:1]:
        uri = "%s&station=%s" % (service, station)
        print("Downloading: %s" % (station,))
        data = download_data(uri)
        df = pd.read_csv(StringIO(data), skiprows=5)
        df['valid'] = pd.to_datetime(df['valid'],format= '%Y-%m-%d %H:%M' ).round('h')
        print(df['valid'])
        # outfn = "%s_%s_%s.txt" % (
        #     station,
        #     startts.strftime("%Y%m%d%H%M"),
        #     endts.strftime("%Y%m%d%H%M"),
        # )
        # out = open(outfn, "w")
        # out.write(data)
        # out.close()


def get_data_from_iem(station_id: Union[str, list, None], start_time: str, end_time: Union[str, None] = None, state:Union[str, list, None] = None, nsrdb: bool = True, nsrdb_key=None, drop: Union[int, float] = 0):
    """
    Get data from Iowa Environmental Mesonet.
    Returns a pandas dataframe with meta info.
    """
    if isinstance(start_time, str):
        startts = parser.parse(start_time).replace(tzinfo=datetime.timezone.utc)
    if isinstance(end_time, str):
        endts = parser.parse(end_time).replace(tzinfo=datetime.timezone.utc)
    if end_time is None or end_time == start_time:
        try:
            time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            endts = startts + datetime.timedelta(hours=1)
        except ValueError:
            endts = startts + datetime.timedelta(days=1)

    # service = SERVICE + "data=all&tz=Etc/UTC&format=comma&latlon=yes&"
    service = SERVICE + "data=tmpc&data=drct&data=sped&data=skyc1&tz=UTC&format=comma&latlon=yes&missing=null&trace=null&"

    service += startts.strftime("year1=%Y&month1=%m&day1=%d&hour1=%H&")
    service += endts.strftime("year2=%Y&month2=%m&day2=%d&hour2=%H&")

    # Two examples of how to specify a list of stations
    stations = None
    if isinstance(state, str):
        stations = get_stations_from_networks(state)
    elif isinstance(state, list):
        stations = []
        for i in state:
            stations += get_stations_from_networks(i)
    if stations is None:
        if isinstance(station_id, str):
            stations = [station_id]
        elif isinstance(station_id, list):
            stations = station_id

    df_container = []
    valid_stations = []
    meta = OrderedDict()
    print("-------------retrieving data from ASOS now--------------")
    for station in stations:
        if random.random() < drop:  # randomly drop some stations if there are too many
            continue
        else:
            uri = "%s&station=%s" % (service, station)
            print("Downloading: %s" % (station,))
            data = download_data(uri)
            df = pd.read_csv(StringIO(data), skiprows=5)
            if not df.empty:
                df['valid'] = pd.to_datetime(df['valid'],format= '%Y-%m-%d %H:%M' ).dt.floor('H')
                df.rename(columns={'valid':'time'}, inplace=True)
                df.set_index('time', inplace=True)
                meta[station] = {'lat': df['lat'].iloc[0], 'lon': df['lon'].iloc[0]}
                df.drop(['station', 'lat', 'lon'], axis=1, inplace=True)
                df = df.groupby("time").last().sort_index().resample(pd.offsets.Hour(1)).asfreq()
                # df['sped'] = df['sped']/2.237  # convert from mph to m/s
                df['tmpc'] = df['tmpc'].astype("pint[degC]")
                df['sped'] = df['sped'].astype("pint[mile per hour]")
                df['drct'] = df['drct'].astype("pint[degrees]")
                df_container.append(df)
                valid_stations.append(station)
    
    if nsrdb:
        print("-------------retrieving data from NSRDB now--------------")
        nsrdb_file = f"/nrel/nsrdb/v3/nsrdb_{startts.year}.h5"
        if nsrdb_key:
            option = {
                'endpoint': 'https://developer.nrel.gov/api/hsds',
                'api_key': nsrdb_key
            }
        else:
            option = {
                'endpoint': 'https://developer.nrel.gov/api/hsds',
                'api_key': 'fm5VsgYKIB3qYrmnuXyTlq05cwUvAIQnafTpSOHx'
            }

        with NSRDBX(nsrdb_file, hsds=True, hsds_kwargs=option) as f:
            gid_list = f.lat_lon_gid(list(zip([meta[station]['lat'] for station in valid_stations], [meta[station]['lon'] for station in valid_stations])))
            timestamp = [startts, endts]
            idx= np.searchsorted(f.time_index, timestamp)
            data = f['ghi', idx[0]:idx[1]:2, gid_list]  # field, timestep, station
        
        for i, df in enumerate(df_container):
            df['ghi'] = data[:, i]
            df['ghi'] = df['ghi'].astype("pint[W/m^2]")
            
    if len(df_container) == 1:
        return df_container[0], meta
    elif len(df_container) > 1:
        return pd.concat(df_container, keys=valid_stations, axis=1), meta


if __name__ == "__main__":
    stations = pd.read_csv(r"C:\Users\test\PycharmProjects\cloudside\texas_asos_stations.csv")
    selected_stations = stations['ID'].tolist()
    selected_stations = [station[1:] for station in selected_stations]
    data = get_data_from_iem(selected_stations, start_time='2020-08-01 12:00:00', end_time=None, state=None, drop=0, nsrdb=True)
    print(data[0].dtypes)
    # print(data[0])