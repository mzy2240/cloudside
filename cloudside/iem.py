# Author: Zeyu Mao (zeyumao2@tamu.edu)

from __future__ import print_function
import json
from logging import warning
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
import warnings
# import pint 
# import pint_pandas
from tqdm import tqdm
import zipfile
import os

# Python 2 and 3: alternative 4
# try:
#     from urllib.request import urlopen
# except ImportError:
#     from urllib2 import urlopen
from urllib.request import urlopen

# Number of attempts to download data
MAX_ATTEMPTS = 10
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


def get_data_from_iem(station_id: Union[str, list, None], start_time: str, end_time: Union[str, None] = None, state:Union[str, list, None] = None, nsrdb: bool = False, nsrdb_key="", drop: Union[int, float] = 0, streamlit=False):
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
    service = SERVICE + "data=tmpf&data=dwpf&data=drct&data=sped&data=skyc1&tz=UTC&format=comma&latlon=yes&missing=null&trace=null&"

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
    invalid = []
    print("-------------retrieving data from ASOS now--------------")
    pbar =  tqdm(stations)
    if streamlit:
        import streamlit as st
        status_text = st.empty()
        pbr = st.progress(0)
        percent_complete  = 0
    for station in pbar:
        if streamlit:
            status_text.text("Downloading: %s" % station)
            percent_complete = percent_complete + 1/len(stations) if percent_complete + 1/len(stations) <=1 else 0.9999
            pbr.progress(percent_complete)
        if random.random() < drop:  # randomly drop some stations if there are too many
            continue
        else:
            uri = "%s&station=%s" % (service, station)
            # print("Downloading: %s" % (station,))
            pbar.set_description("Downloading: %s" % station)
            data = download_data(uri)
            df = pd.read_csv(StringIO(data), skiprows=5)
            if not df.empty:
                df['valid'] = pd.to_datetime(df['valid'],format= '%Y-%m-%d %H:%M' ).dt.floor('H')
                df.rename(columns={'valid':'time'}, inplace=True)
                df.set_index('time', inplace=True)
                if df.isnull().sum().sum() >= df.shape[0]:  # remove the station if it has too many missing values
                    continue
                else:
                    meta[station] = {'lat': df['lat'].iloc[0], 'lon': df['lon'].iloc[0]}
                    df.drop(['station', 'lat', 'lon'], axis=1, inplace=True)
                    df = df.groupby("time").last().sort_index().resample(pd.offsets.Hour(1)).asfreq()
                    # df['sped'] = df['sped']/2.237  # convert from mph to m/s
                    # df['tmpc'] = df['tmpc'].astype("pint[degC]")
                    # df['sped'] = df['sped'].astype("pint[mile per hour]")
                    # df['drct'] = df['drct'].astype("pint[degrees]")
                    df_container.append(df)
                    valid_stations.append(station)
    
    if streamlit:
        if percent_complete != 1:
            pbr.progress(0.9999)
        status_text.text("Done!")

    if nsrdb:
        print("-------------retrieving data from NSRDB now--------------")
        if streamlit:
            status_text.text("Downloading from NSRDB ...")
        nsrdb_file = f"/nrel/nsrdb/v3/nsrdb_{startts.year}.h5"
        if nsrdb_key:
            option = {
                'endpoint': 'https://developer.nrel.gov/api/hsds',
                'api_key': nsrdb_key
            }
        else:
            option = {
                'endpoint': 'https://developer.nrel.gov/api/hsds',
                'api_key': 'ib4rRdgnLqSx8W0L4FylazXw5zsKXuCWB71z7TkX'
            }

        with NSRDBX(nsrdb_file, hsds=True, hsds_kwargs=option) as f:
            lat_lon = np.array(list(zip([meta[station]['lat'] for station in valid_stations], [meta[station]['lon'] for station in valid_stations])))
            dist, gids = f.tree.query(lat_lon)
            dist_check = dist > f.distance_threshold
 
            if np.any(dist_check):
                # remove stations outside of the NSRDB distance threshold
                gids = np.delete(gids, dist_check)
                valid_stations = [station for (station, remove) in zip(valid_stations, dist_check) if not remove]
                df_container = [df for (df, remove) in zip(df_container, dist_check) if not remove]

                for i, (key, remove) in enumerate(zip(list(meta), dist_check)):
                    if remove:
                        del meta[key]

            timestamp = [startts, endts]
            idx= np.searchsorted(f.time_index, timestamp)
            try:
                data = f['ghi', idx[0]:idx[1]:2, gids]  # field, timestep, station
            
                for i, df in enumerate(tqdm(df_container, desc="Processing SRD")):
                    try:
                        df['ghi'] = data[:, i]
                    except ValueError:
                        invalid.append(i)
                        # df['ghi'] = df['ghi'].astype("pint[W/m^2]")
            except OSError:
                print("Too many requests for the current API key.")

    if invalid:
        # remove stations with invalid data
        for i in invalid:
            del meta[valid_stations[i]]
        df_container = [j for i, j in enumerate(df_container) if i not in invalid]
        valid_stations = [j for i, j in enumerate(valid_stations) if i not in invalid]

    if len(df_container) == 1:
        return df_container[0], meta
    elif len(df_container) > 1:
        return pd.concat(df_container, keys=valid_stations, axis=1), meta

def save_excel(df, name, replace_nan):
    df = df.add_prefix('K')  # add prefix to all the column names
    df = df.droplevel(1, axis=1)  # remove the second level of the column names
    df.index = df.index.rename("Date and Time (UTC, ISO8601 Format)")

    df.index = df.index.to_series().apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z')

    # now save the data to excel
    writer = pd.ExcelWriter(f"{name}.xlsx", engine='xlsxwriter')
    df.to_excel(writer, float_format="%.1f", startrow=1, na_rep=str(replace_nan), sheet_name='Sheet1')
    worksheet = writer.sheets['Sheet1']
    worksheet.write('A1', "PWOPFTimePoint")
    writer.save()

def save_data(df, cloud_type="Categorical", replace_nan=-9999):
    """
    Save weather data to excel files in the format that PW supports.
    """
    temperature = df.loc[:, (slice(None), "tmpf")].copy()
    save_excel(temperature, "temperature", replace_nan)
    wind_speed = df.loc[:, (slice(None), "sped")].copy()
    save_excel(wind_speed, "wind_speed", replace_nan)
    wind_dirc = df.loc[:, (slice(None), "drct")].copy()
    save_excel(wind_dirc, "wind_direction", replace_nan)
    dew_point = df.loc[:, (slice(None), "dwpf")].copy()
    save_excel(dew_point, "dew_point", replace_nan)
    cloud_coverage = df.loc[:, (slice(None), "skyc1")].copy()
    if cloud_type == "Numerical":
        cloud_coverage = cloud_coverage.replace(
            {
                "SKC": 0,
                "CLR": 0,
                "FEW": 1,
                "SCT": 2,
                "BKN": 3,
                "OVC": 4,
                "VV": -9999
            }
        )
    save_excel(cloud_coverage, "cloud_coverage", replace_nan)
    try:
        solar_radiation = df.loc[:, (slice(None), "ghi")].copy()
        save_excel(solar_radiation, "solar_radiation", replace_nan)
    except KeyError:
        pass
    with zipfile.ZipFile("weather_data.zip", mode="w") as archive:
        archive.write("temperature.xlsx")
        os.remove("temperature.xlsx")
        archive.write("wind_speed.xlsx")
        os.remove("wind_speed.xlsx")
        archive.write("wind_direction.xlsx")
        os.remove("wind_direction.xlsx")
        archive.write("dew_point.xlsx")
        os.remove("dew_point.xlsx")
        archive.write("cloud_coverage.xlsx")
        os.remove("cloud_coverage.xlsx")
        try:
            archive.write("solar_radiation.xlsx")
            os.remove("solar_radiation.xlsx")
        except NameError:
            pass
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    stations = pd.read_csv(r"C:\Users\test\PycharmProjects\cloudside\texas_asos_stations.csv")
    selected_stations = stations['ID'].tolist()
    selected_stations = [station[1:] for station in selected_stations]
    data = get_data_from_iem(selected_stations, start_time='2020-06-01', end_time='2020-06-02', state="TX", drop=0, nsrdb=True)
    # save_data(data[0])
    # print(data[0])