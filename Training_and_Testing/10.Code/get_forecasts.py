import forecastio
import datetime
import pytz as tz
import json as js
import pandas as pd
from pandas.io.json import json_normalize
import argparse
from collections import defaultdict
from math import ceil
import sys
import logging
import forecastiocreds
'''
This script get past 30 days for forecast data,
in addition to 3 days into the future.

Note: missing properties omitted from Dark Sky's datapoint object are imputed
with 0, e.g. wind speed.
'''


forecast_key = forecastiocreds.forecastio_key
#print(forecastiocreds.forecastio_key)
SECONDSPERHOUR = 3600
#OUPUT_FILENAME = "lagged_forecasts.csv"
failDict = defaultdict(int)



def main(lat, lng, forecast_key, city, Job_Dir, out_file):
   # logging.basicConfig(filename='forecast.log',
    #                    format='%(asctime)s %(message)s',
     #                   datefmt='%m/%d/%Y %I:%M:%S %p',
      #                  level=logging.INFO)
    getForecasts(lat, lng, forecast_key, city, Job_Dir, out_file)
    #logging.info('Failure counts: ' + str(dict(failDict)))
    logging.info('====== Finished fetching forecasts: {} ======'.format(city.title()))

def getAttr(data, variable, default):
    value = getattr(data, variable, None)
    if value is None:
        failDict[variable] += 1
        failDict['total'] += 1
        return default
    return value

def getForecasts(lat, lng, forecast_key, city, Job_Dir, out_file):
    logging.info('====== Started fetching forecasts: {} ======='.format(city.title()))
    print("Started fetching forecasts for " + city.title() + ' ({}, {})'.format(lat, lng))

    #utcnow is a naive datetime, so have to set the timezone to utc
    current_time = datetime.datetime.utcnow().replace(tzinfo=tz.utc)
    #forecast io returns weather data for time provided and 24 hrs before,
    #so we request 29 days back instead of 30
    date = 365*4 #download four years of data when first starting a city 
    start = (current_time - datetime.timedelta(days=date)).replace(minute=0, second=0, microsecond=0)
    endtime = (current_time + datetime.timedelta(days=0)).replace(minute=0, second=0, microsecond=0)
    i = 0
    window_count = 0
    json = []
    timezone=None

    while i <= (date+0): #looks up forecasts for each hour for (date+0) days - date days in the past and 0 days in the future
        hour = 0
        lookUpTime = start + datetime.timedelta(days=i)
        forecast = forecastio.load_forecast(
            forecast_key, lat, lng, time=lookUpTime)
        timezone=forecast.json['timezone']

        for hourlyData in forecast.hourly().data:
            thisHour = hourlyData.time.replace(tzinfo=tz.utc)
            if thisHour < start or thisHour >= endtime:
            #Forecast.io returns data starting from midnight prior to the day requested.
            #Some of this might be extraneous, so must check to see if the time
            #of the hourly object is before the start time. Note start time is
            #one day ahead.
                continue

            window = int(ceil((thisHour - current_time
                            ).total_seconds()/SECONDSPERHOUR))
            if window >= 0:
                window_count += 1
            json.append({'hournumber': hour,
                            'year': current_time.year,
                            'wind_speed': getAttr(hourlyData, 'windSpeed', 0),
                            'hr': thisHour,
                            'relative_humidity': getAttr(hourlyData, 'humidity', 0) * 100,
                            'drybulb_fahrenheit': getAttr(hourlyData, 'temperature', 0),
                            'hourly_precip': getAttr(hourlyData, 'precipIntensity', 0),
                            'precip_probability': getAttr(hourlyData, 'precipProbability', 0),
                            'apparent_temperature': getAttr(hourlyData, 'apparentTemperature', 0),
                            'weather_type': getAttr(hourlyData, 'icon', 0),
                            'dew_point': getAttr(hourlyData, 'dewPoint', 0),
                            'wind_gust': getAttr(hourlyData, 'windGust', 0),
                            'wind_bearing': getAttr(hourlyData, 'windBearing', 0),
                            'visibility': getAttr(hourlyData, 'visibility', 0),
                            'cloud_cover': getAttr(hourlyData, 'cloudCover', 0),
                            'pressure': getAttr(hourlyData, 'pressure', 0),

                            'dod1_drybulb_fahrenheit': 0,
                            'dod2_drybulb_fahrenheit': 0,
                            'dod3_drybulb_fahrenheit': 0,
                            'wow1_drybulb_fahrenheit': 0,
                            'wow2_drybulb_fahrenheit': 0,
                            'hour_count_since_precip': -1,
                            'precip_hour_cnt_in_last_1_week': 0,
                            'precip_hour_cnt_in_last_1_day': 0,
                            'precip_hour_cnt_in_last_3_day': 0,
                            'window': int(window)})
            hour += 1
        i+=1

    df = pd.DataFrame(json)
    #iterates from 720 to 791, the weather forecasts for the next 3 days
    relevant = len(df) - len(df[df['window']>=0])


##STP: drop 109-146
    # #legacy code... not sure what is going on
    # for i in range(relevant, len(df)):
    #     df.ix[i,'dod1_drybulb_fahrenheit'] = df.ix[
    #         i,'drybulb_fahrenheit'] - df.ix[i-24,'drybulb_fahrenheit']
    #     df.ix[i,'dod2_drybulb_fahrenheit'] = df.ix[
    #         i,'drybulb_fahrenheit'] - df.ix[i-48,'drybulb_fahrenheit']
    #     df.ix[i,'dod3_drybulb_fahrenheit'] = df.ix[
    #         i,'drybulb_fahrenheit'] - df.ix[i-72,'drybulb_fahrenheit']
    #     df.ix[i,'wow1_drybulb_fahrenheit'] = df.ix[
    #         i,'drybulb_fahrenheit'] - df.ix[i-168,'drybulb_fahrenheit']
    #     df.ix[i,'wow2_drybulb_fahrenheit'] = df.ix[
    #         i,'drybulb_fahrenheit'] - df.ix[i-336,'drybulb_fahrenheit']
    #     for j in range(1,i):
    #         if df.ix[i - j,'hourly_precip'] > .001:
    #             df.ix[i,'hour_count_since_precip'] = j
    #             break
    #     precip_1wk_cnt = 0
    #     precip_1d_cnt = 0
    #     precip_3d_cnt = 0
    #     for k in range(1, 168):
    #         if df.ix[i - k,'hourly_precip'] > .001:
    #             if k <= 24:
    #                 precip_1d_cnt += 1
    #                 precip_1wk_cnt+=1
    #                 precip_3d_cnt+=1
    #             elif k <= 72:
    #                 precip_1wk_cnt+=1
    #                 precip_3d_cnt+=1
    #             else:
    #                 precip_1wk_cnt+=1
    #     df.ix[i,'precip_hour_cnt_in_last_1_week'] = precip_1wk_cnt
    #     df.ix[i,'precip_hour_cnt_in_last_1_day'] = precip_1d_cnt
    #     df.ix[i,'precip_hour_cnt_in_last_3_day'] = precip_3d_cnt
    # # not sure what this was doing, but it doesn't make sense
    # # changing it to all rows >= 0, which keeps all rows for the next 72 hrs
    # # old: df = df[(df['window'] >= -5) & (df['window'] < 66)]
    # df = df[(df['window'] >= 0)]
    # df = df.drop('window', 1)


    # Make sure the local timezone is used.
    localTime = current_time.astimezone(tz.timezone(timezone))
    df['prediction_timestamp'] = localTime.strftime('%b %d, %Y %H:%M')
    df[['hr','wind_speed','relative_humidity','hourly_precip','drybulb_fahrenheit' , 'precip_probability'
                            ,'apparent_temperature'
                            ,'weather_type'
                            ,'dew_point'
                            ,'wind_gust'
                            ,'wind_bearing'
                            ,'visibility'
                            ,'cloud_cover'
                            ,'pressure']].to_csv(
           Job_Dir + '/weather.csv',index=False)
    # df = df.drop('prediction_timestamp', 1)

    # # CELL_ID_FILENAME = city + "/cell_ids.txt"
    # # f = open(CELL_ID_FILENAME)
    # # #cell_ids = pd.read_csv(CELL_ID_FILENAME)
    # # #cell_ids = cell_ids.ix[:,'cell_id']
    # # #geometry is called cell_id, replaced census tract
    # # cell_ids = pd.DataFrame([ids.strip() for ids in f.readlines()])
    # # cell_ids.columns = ['cell_id']
    # # cell_ids['key'] = 1
    # # df['key'] = 1
    # # final_joined = pd.merge(df,cell_ids,on='key')
    # # final_joined = final_joined.drop(['key'],1)
    # # df = final_joined

    # cols = ['cell_id', 'dt', 'crime_count', 'place_holder1', 'place_holder2',
    # 'year', 'hournumber', 'hourstart', 'wind_speed', 'drybulb_fahrenheit', 'hourly_precip',
    # 'relative_humidity', 'fz', 'ra', 'ts', 'br', 'sn', 'hz', 'dz', 'pl', 'fg', 'sa', 'up',
    # 'fu', 'sq', 'gs', 'dod1_drybulb_fahrenheit', 'dod2_drybulb_fahrenheit', 'dod3_drybulb_fahrenheit',
    # 'wow1_drybulb_fahrenheit',  'wow2_drybulb_fahrenheit', 'precip_hour_cnt_in_last_1_day', 'precip_hour_cnt_in_last_3_day',
    # 'precip_hour_cnt_in_last_1_week', 'hour_count_since_precip', 'month_of_year', 'day_of_week']

    # df['month_of_year']=pd.DatetimeIndex(df['dt']).month
    # df['day_of_week']=pd.DatetimeIndex(df['dt']).dayofweek
    # df['hourstart'] = df['hournumber']
    # df['crime_count'] = 0
    # df['fz'] = 0
    # df['ra'] = 0
    # df['ts'] = 0
    # df['br'] = 0
    # df['sn'] = 0
    # df['hz'] = 0
    # df['dz'] = 0
    # df['pl'] = 0
    # df['fg'] = 0
    # df['sa'] = 0
    # df['up'] = 0
    # df['fu'] = 0
    # df['sq'] = 0
    # df['gs'] = 0
    # df['place_holder1'] = 0
    # df['place_holder2'] = 0
    # df = df[cols]
    # df = df.sort_values(by=['cell_id','dt','hournumber'])
    # df.to_csv("/".join([city, OUPUT_FILENAME]), index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
   # forecast_key = sys.argv[0]
#    parser.add_argument("key", type=str,
 #        help="Key for forecast io")
    parser.add_argument("city", type=str,
        help="directory containing city data")
    parser.add_argument("out_file", type=str,
        help="output filename")
    parser.add_argument("lat", type=float,
        help="Latitude")
    parser.add_argument("lng", type=float,
        help="Longitude")
    parser.add_argument("Job_Dir", type=str,
        help="city data loader job directory")
    args = parser.parse_args()

   # with open(args.city + '/' + CREDFILE, 'r') as f:
    #    lat, lng, key = [x.strip() for x in f.readlines()]

    main(args.lat, args.lng, forecast_key, args.city, args.Job_Dir, args.out_file)
    print("Completed Fetching Forecasts")
