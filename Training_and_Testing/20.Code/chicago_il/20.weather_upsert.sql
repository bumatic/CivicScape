insert into all_weather

WITH 
weather_deltas AS (
  select ts.ts as hr
  , LAG(w.drybulb_fahrenheit, 24*1) OVER( order by ts.ts) as dod1_drybulb_fahrenheit
  , LAG(w.drybulb_fahrenheit, 24*2) OVER( order by ts.ts) as dod2_drybulb_fahrenheit
  , LAG(w.drybulb_fahrenheit, 24*3) OVER( order by ts.ts) as dod3_drybulb_fahrenheit
  , LAG(w.drybulb_fahrenheit, 24*7*1) OVER( order by ts.ts) as wow1_drybulb_fahrenheit
  , LAG(w.drybulb_fahrenheit, 24*7*2) OVER( order by ts.ts) as wow2_drybulb_fahrenheit  
  from generate_series 
    ( '01-01-2010'::timestamp - INTERVAL '2 week'
    , '01-01-2018'::timestamp
    , '1 hour' ::interval) as ts
  left join weather_import w on ts.ts = w.hr
),  
hour_prec_history AS (
 select hourstart_series2 as hr
  , sum (
    case when w.hr between hourstart_series2 - INTERVAL '1 day' and hourstart_series2 then 1 else 0 end
    ) as precip_hour_cnt_in_last_1_day
  , sum (
    case when w.hr between hourstart_series2 - INTERVAL '3 day' and hourstart_series2 then 1 else 0 end
    ) as precip_hour_cnt_in_last_3_day
  , sum (
    case when w.hr between hourstart_series2 - INTERVAL '1 week' and hourstart_series2 then 1 else 0 end
    ) as precip_hour_cnt_in_last_1_week   
  , EXTRACT(EPOCH FROM hourstart_series2 - max(w.hr))/3600 as hour_count_since_precip
  from generate_series ( 
      '01-01-2010' ::timestamp
      , '01-01-2018' ::timestamp
      , '1 hour' ::interval) hourstart_series2
  inner join weather_import w on w.hr between hourstart_series2 - INTERVAL '8 week' and hourstart_series2
  where w.hourly_precip > 0
  group by hourstart_series2
)

SELECT weather_import.hr, case when drybulb_fahrenheit < -20 then 1 else 0 end as "temp_<_-20", case when drybulb_fahrenheit between -20 and -11 then 1 else 0 end as "temp_-20_to_-11", case when drybulb_fahrenheit between -10 and -1 then 1 else 0 end as "temp_-10_to_-1", case when drybulb_fahrenheit between 0 and 9 then 1 else 0 end as "temp_0_to_9", case when drybulb_fahrenheit between 10 and 19 then 1 else 0 end as "temp_10_to_19", case when drybulb_fahrenheit between 20 and 29 then 1 else 0 end as "temp_20_to_29", case when drybulb_fahrenheit between 30 and 39 then 1 else 0 end as "temp_30_to_39", case when drybulb_fahrenheit between 40 and 49 then 1 else 0 end as "temp_40_to_49", case when drybulb_fahrenheit between 50 and 59 then 1 else 0 end as "temp_50_to_59", case when drybulb_fahrenheit between 60 and 69 then 1 else 0 end as "temp_60_to_69", case when drybulb_fahrenheit between 70 and 79 then 1 else 0 end as "temp_70_to_79", case when drybulb_fahrenheit between 80 and 89 then 1 else 0 end as "temp_80_to_89", case when drybulb_fahrenheit between 90 and 99 then 1 else 0 end as "temp_90_to_99", case when drybulb_fahrenheit between 100 and 109 then 1 else 0 end as "temp_100_to_109", case when drybulb_fahrenheit between 110 and 119 then 1 else 0 end as "temp_110_to_119", case when drybulb_fahrenheit >= 120 then 1 else 0 end as "temp_>=_120", case when wind_speed < 5 then 1 else 0 end as "wind_<_5", case when wind_speed between 5 and 9 then 1 else 0 end as "wind_5_to_9", case when wind_speed between 10 and 14 then 1 else 0 end as "wind_10_to_14", case when wind_speed between 15 and 19 then 1 else 0 end as "wind_15_to_19", case when wind_speed between 20 and 24 then 1 else 0 end as "wind_20_to_24", case when wind_speed between 25 and 29 then 1 else 0 end as "wind_25_to_29", case when wind_speed between 30 and 34 then 1 else 0 end as "wind_30_to_34", case when wind_speed between 35 and 39 then 1 else 0 end as "wind_35_to_39", case when wind_speed between 40 and 44 then 1 else 0 end as "wind_40_to_44", case when wind_speed between 45 and 49 then 1 else 0 end as "wind_45_to_49", case when wind_speed >= 50 then 1 else 0 end as "wind_>=_50", case when hourly_precip < 0 then 1 else 0 end as "rain_<_0", case when hourly_precip between 0.0 and 0.0999 then 1 else 0 end as "rain_0.0_to_0.0999", case when hourly_precip between 0.125 and 0.2249 then 1 else 0 end as "rain_0.125_to_0.2249", case when hourly_precip between 0.25 and 0.3499 then 1 else 0 end as "rain_0.25_to_0.3499", case when hourly_precip between 0.375 and 0.4749 then 1 else 0 end as "rain_0.375_to_0.4749", case when hourly_precip between 0.5 and 0.5999 then 1 else 0 end as "rain_0.5_to_0.5999", case when hourly_precip between 0.625 and 0.7249 then 1 else 0 end as "rain_0.625_to_0.7249", case when hourly_precip between 0.75 and 0.8499 then 1 else 0 end as "rain_0.75_to_0.8499", case when hourly_precip between 0.875 and 0.9749 then 1 else 0 end as "rain_0.875_to_0.9749", case when hourly_precip between 1.0 and 1.0999 then 1 else 0 end as "rain_1.0_to_1.0999", case when hourly_precip >= 1 then 1 else 0 end as "rain_>=_1", case when relative_humidity < 10 then 1 else 0 end as "humidity_<_10", case when relative_humidity between 10 and 19 then 1 else 0 end as "humidity_10_to_19", case when relative_humidity between 20 and 29 then 1 else 0 end as "humidity_20_to_29", case when relative_humidity between 30 and 39 then 1 else 0 end as "humidity_30_to_39", case when relative_humidity between 40 and 49 then 1 else 0 end as "humidity_40_to_49", case when relative_humidity between 50 and 59 then 1 else 0 end as "humidity_50_to_59", case when relative_humidity between 60 and 69 then 1 else 0 end as "humidity_60_to_69", case when relative_humidity between 70 and 79 then 1 else 0 end as "humidity_70_to_79", case when relative_humidity between 80 and 89 then 1 else 0 end as "humidity_80_to_89", case when relative_humidity between 90 and 99 then 1 else 0 end as "humidity_90_to_99", case when relative_humidity >= 100 then 1 else 0 end as "humidity_>=_100",

case when weather_deltas.wow1_drybulb_fahrenheit = 0 then 1 else 0 end as "wow_1_change_=0",
case when weather_deltas.wow1_drybulb_fahrenheit between 1 and 5 then 1 else 0 end as "wow_1_change_1_to_5",
case when weather_deltas.wow1_drybulb_fahrenheit between 5 and 10 then 1 else 0 end as "wow_1_change_5_to_10",
case when weather_deltas.wow1_drybulb_fahrenheit between 10 and 20 then 1 else 0 end as "wow_1_change_10_to_20",
case when weather_deltas.wow1_drybulb_fahrenheit between 20 and 30 then 1 else 0 end as "wow_1_change_20_to_30",
case when weather_deltas.wow1_drybulb_fahrenheit between 30 and 40 then 1 else 0 end as "wow_1_change_30_to_40",
case when weather_deltas.wow1_drybulb_fahrenheit between 40 and 50 then 1 else 0 end as "wow_1_change_40_to_50",
case when weather_deltas.wow1_drybulb_fahrenheit >= 50 then 1 else 0 end as "wow_1_change_>50",
case when weather_deltas.wow1_drybulb_fahrenheit between -1 and -5 then 1 else 0 end as "wow_1_change_neg1_to_neg5",
case when weather_deltas.wow1_drybulb_fahrenheit between -5 and -10 then 1 else 0 end as "wow_1_change_neg5_to_neg10",
case when weather_deltas.wow1_drybulb_fahrenheit between -10 and -20 then 1 else 0 end as "wow_1_change_neg10_to_neg20",
case when weather_deltas.wow1_drybulb_fahrenheit between -20 and -30 then 1 else 0 end as "wow_1_change_neg20_to_neg30",
case when weather_deltas.wow1_drybulb_fahrenheit between -30 and -40 then 1 else 0 end as "wow_1_change_neg30_to_neg40",
case when weather_deltas.wow1_drybulb_fahrenheit between -40 and -50 then 1 else 0 end as "wow_1_change_neg40_to_neg50",
case when weather_deltas.wow1_drybulb_fahrenheit <= -50 then 1 else 0 end as "wow_1_change_>neg50",
case when weather_deltas.wow2_drybulb_fahrenheit = 0 then 1 else 0 end as "wow_2_change_=0",
case when weather_deltas.wow2_drybulb_fahrenheit between 1 and 5 then 1 else 0 end as "wow_2_change_1_to_5",
case when weather_deltas.wow2_drybulb_fahrenheit between 5 and 10 then 1 else 0 end as "wow_2_change_5_to_10",
case when weather_deltas.wow2_drybulb_fahrenheit between 10 and 20 then 1 else 0 end as "wow_2_change_10_to_20",
case when weather_deltas.wow2_drybulb_fahrenheit between 20 and 30 then 1 else 0 end as "wow_2_change_20_to_30",
case when weather_deltas.wow2_drybulb_fahrenheit between 30 and 40 then 1 else 0 end as "wow_2_change_30_to_40",
case when weather_deltas.wow2_drybulb_fahrenheit between 40 and 50 then 1 else 0 end as "wow_2_change_40_to_50",
case when weather_deltas.wow2_drybulb_fahrenheit >= 50 then 1 else 0 end as "wow_2_change_>50",
case when weather_deltas.wow2_drybulb_fahrenheit between -1 and -5 then 1 else 0 end as "wow_2_change_neg1_to_neg5",
case when weather_deltas.wow2_drybulb_fahrenheit between -5 and -10 then 1 else 0 end as "wow_2_change_neg5_to_neg10",
case when weather_deltas.wow2_drybulb_fahrenheit between -10 and -20 then 1 else 0 end as "wow_2_change_neg10_to_neg20",
case when weather_deltas.wow2_drybulb_fahrenheit between -20 and -30 then 1 else 0 end as "wow_2_change_neg20_to_neg30",
case when weather_deltas.wow2_drybulb_fahrenheit between -30 and -40 then 1 else 0 end as "wow_2_change_neg30_to_neg40",
case when weather_deltas.wow2_drybulb_fahrenheit between -40 and -50 then 1 else 0 end as "wow_2_change_neg40_to_neg50",
case when weather_deltas.wow2_drybulb_fahrenheit <= -50 then 1 else 0 end as "wow_2_change_>neg50",

case when weather_deltas.dod1_drybulb_fahrenheit = 0 then 1 else 0 end as "dod_1_change_=0",
case when weather_deltas.dod1_drybulb_fahrenheit between 1 and 5 then 1 else 0 end as "dod_1_change_1_to_5",
case when weather_deltas.dod1_drybulb_fahrenheit between 5 and 10 then 1 else 0 end as "dod_1_change_5_to_10",
case when weather_deltas.dod1_drybulb_fahrenheit between 10 and 20 then 1 else 0 end as "dod_1_change_10_to_20",
case when weather_deltas.dod1_drybulb_fahrenheit between 20 and 30 then 1 else 0 end as "dod_1_change_20_to_30",
case when weather_deltas.dod1_drybulb_fahrenheit between 30 and 40 then 1 else 0 end as "dod_1_change_30_to_40",
case when weather_deltas.dod1_drybulb_fahrenheit between 40 and 50 then 1 else 0 end as "dod_1_change_40_to_50",
case when weather_deltas.dod1_drybulb_fahrenheit >= 50 then 1 else 0 end as "dod_1_change_>50",
case when weather_deltas.dod1_drybulb_fahrenheit between -1 and -5 then 1 else 0 end as "dod_1_change_neg1_to_neg5",
case when weather_deltas.dod1_drybulb_fahrenheit between -5 and -10 then 1 else 0 end as "dod_1_change_neg5_to_neg10",
case when weather_deltas.dod1_drybulb_fahrenheit between -10 and -20 then 1 else 0 end as "dod_1_change_neg10_to_neg20",
case when weather_deltas.dod1_drybulb_fahrenheit between -20 and -30 then 1 else 0 end as "dod_1_change_neg20_to_neg30",
case when weather_deltas.dod1_drybulb_fahrenheit between -30 and -40 then 1 else 0 end as "dod_1_change_neg30_to_neg40",
case when weather_deltas.dod1_drybulb_fahrenheit between -40 and -50 then 1 else 0 end as "dod_1_change_neg40_to_neg50",
case when weather_deltas.dod1_drybulb_fahrenheit <= -50 then 1 else 0 end as "dod_1_change_>neg50",
case when weather_deltas.dod2_drybulb_fahrenheit = 0 then 1 else 0 end as "dod_2_change_=0",
case when weather_deltas.dod2_drybulb_fahrenheit between 1 and 5 then 1 else 0 end as "dod_2_change_1_to_5",
case when weather_deltas.dod2_drybulb_fahrenheit between 5 and 10 then 1 else 0 end as "dod_2_change_5_to_10",
case when weather_deltas.dod2_drybulb_fahrenheit between 10 and 20 then 1 else 0 end as "dod_2_change_10_to_20",
case when weather_deltas.dod2_drybulb_fahrenheit between 20 and 30 then 1 else 0 end as "dod_2_change_20_to_30",
case when weather_deltas.dod2_drybulb_fahrenheit between 30 and 40 then 1 else 0 end as "dod_2_change_30_to_40",
case when weather_deltas.dod2_drybulb_fahrenheit between 40 and 50 then 1 else 0 end as "dod_2_change_40_to_50",
case when weather_deltas.dod2_drybulb_fahrenheit >= 50 then 1 else 0 end as "dod_2_change_>50",
case when weather_deltas.dod2_drybulb_fahrenheit between -1 and -5 then 1 else 0 end as "dod_2_change_neg1_to_neg5",
case when weather_deltas.dod2_drybulb_fahrenheit between -5 and -10 then 1 else 0 end as "dod_2_change_neg5_to_neg10",
case when weather_deltas.dod2_drybulb_fahrenheit between -10 and -20 then 1 else 0 end as "dod_2_change_neg10_to_neg20",
case when weather_deltas.dod2_drybulb_fahrenheit between -20 and -30 then 1 else 0 end as "dod_2_change_neg20_to_neg30",
case when weather_deltas.dod2_drybulb_fahrenheit between -30 and -40 then 1 else 0 end as "dod_2_change_neg30_to_neg40",
case when weather_deltas.dod2_drybulb_fahrenheit between -40 and -50 then 1 else 0 end as "dod_2_change_neg40_to_neg50",
case when weather_deltas.dod2_drybulb_fahrenheit <= -50 then 1 else 0 end as "dod_2_change_>neg50",
case when weather_deltas.dod3_drybulb_fahrenheit = 0 then 1 else 0 end as "dod_3_change_=0",
case when weather_deltas.dod3_drybulb_fahrenheit between 1 and 5 then 1 else 0 end as "dod_3_change_1_to_5",
case when weather_deltas.dod3_drybulb_fahrenheit between 1 and 5 then 1 else 0 end as "dod_3_change_5_to_10",
case when weather_deltas.dod3_drybulb_fahrenheit between 10 and 20 then 1 else 0 end as "dod_3_change_10_to_20",
case when weather_deltas.dod3_drybulb_fahrenheit between 20 and 30 then 1 else 0 end as "dod_3_change_20_to_30",
case when weather_deltas.dod3_drybulb_fahrenheit between 30 and 40 then 1 else 0 end as "dod_3_change_30_to_40",
case when weather_deltas.dod3_drybulb_fahrenheit between 40 and 50 then 1 else 0 end as "dod_3_change_40_to_50",
case when weather_deltas.dod3_drybulb_fahrenheit >= 50 then 1 else 0 end as "dod_3_change_>50",
case when weather_deltas.dod3_drybulb_fahrenheit between -1 and -5 then 1 else 0 end as "dod_3_change_neg1_to_neg5",
case when weather_deltas.dod3_drybulb_fahrenheit between -5 and -10 then 1 else 0 end as "dod_3_change_neg5_to_neg10",
case when weather_deltas.dod3_drybulb_fahrenheit between -10 and -20 then 1 else 0 end as "dod_3_change_neg10_to_neg20",
case when weather_deltas.dod3_drybulb_fahrenheit between -20 and -30 then 1 else 0 end as "dod_3_change_neg20_to_neg30",
case when weather_deltas.dod3_drybulb_fahrenheit between -30 and -40 then 1 else 0 end as "dod_3_change_neg30_to_neg40",
case when weather_deltas.dod3_drybulb_fahrenheit between -40 and -50 then 1 else 0 end as "dod_3_change_neg40_to_neg50",
case when weather_deltas.dod3_drybulb_fahrenheit <= -50 then 1 else 0 end as "dod_3_change_>neg50",

case when hour_prec_history.precip_hour_cnt_in_last_1_day = 0 then 1 else 0 end as "rain_1d_count_=0",
case when hour_prec_history.precip_hour_cnt_in_last_1_day between 1 and 5 then 1 else 0 end as "rain_1d_count_1_to_5",
case when hour_prec_history.precip_hour_cnt_in_last_1_day between 5 and 10 then 1 else 0 end as "rain_1d_count_5_to_10",
case when hour_prec_history.precip_hour_cnt_in_last_1_day between 10 and 20 then 1 else 0 end as "rain_1d_count_10_to_20",
case when hour_prec_history.precip_hour_cnt_in_last_1_day between 20 and 24 then 1 else 0 end as "rain_1d_count_20_to_24",
case when hour_prec_history.precip_hour_cnt_in_last_3_day = 0 then 1 else 0 end as "rain_3day_count_=0",
case when hour_prec_history.precip_hour_cnt_in_last_3_day between 1 and 5 then 1 else 0 end as "rain_3day_count_1_to_5",
case when hour_prec_history.precip_hour_cnt_in_last_3_day between 5 and 10 then 1 else 0 end as "rain_3day_count_5_to_10",
case when hour_prec_history.precip_hour_cnt_in_last_3_day between 10 and 20 then 1 else 0 end as "rain_3day_count_10_to_20",
case when hour_prec_history.precip_hour_cnt_in_last_3_day between 20 and 24 then 1 else 0 end as "rain_3day_count_20_to_24",
case when hour_prec_history.precip_hour_cnt_in_last_3_day between 24 and 30 then 1 else 0 end as "rain_3day_count_24_to_30",
case when hour_prec_history.precip_hour_cnt_in_last_3_day between 30 and 36 then 1 else 0 end as "rain_3day_count_30_to_36",
case when hour_prec_history.precip_hour_cnt_in_last_3_day between 37 and 47 then 1 else 0 end as "rain_3day_count_37_to_47",
case when hour_prec_history.precip_hour_cnt_in_last_3_day between 48 and 57 then 1 else 0 end as "rain_3day_count_48_to_57",
case when hour_prec_history.precip_hour_cnt_in_last_3_day between 58 and 65 then 1 else 0 end as "rain_3day_count_58_to_65",
case when hour_prec_history.precip_hour_cnt_in_last_3_day between 66 and 72 then 1 else 0 end as "rain_3day_count_66_to_72",
case when hour_prec_history.precip_hour_cnt_in_last_1_week = 0 then 1 else 0 end as "rain_1week_count_=0",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 1 and 5 then 1 else 0 end as "rain_1week_count_1_to_5",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 5 and 10 then 1 else 0 end as "rain_1week_count_5_to_10",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 10 and 20 then 1 else 0 end as "rain_1week_count_10_to_20",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 20 and 24 then 1 else 0 end as "rain_1week_count_20_to_24",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 24 and 30 then 1 else 0 end as "rain_1week_count_24_to_30",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 30 and 36 then 1 else 0 end as "rain_1week_count_30_to_36",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 37 and 47 then 1 else 0 end as "rain_1week_count_37_to_47",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 48 and 57 then 1 else 0 end as "rain_1week_count_48_to_57",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 58 and 65 then 1 else 0 end as "rain_1week_count_58_to_65",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 66 and 72 then 1 else 0 end as "rain_1week_count_66_to_72",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 73 and 96 then 1 else 0 end as "rain_1week_count_73_to_96",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 97 and 120 then 1 else 0 end as "rain_1week_count_97_to_120",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 121 and 144 then 1 else 0 end as "rain_1week_count_121_to_144",
case when hour_prec_history.precip_hour_cnt_in_last_1_week between 145 and 168 then 1 else 0 end as "rain_1week_count_145_to_168",

case when hour_prec_history.hour_count_since_precip = 0 then 1 else 0 end as "since_rain_count_=0",
case when hour_prec_history.hour_count_since_precip between 1 and 5 then 1 else 0 end as "since_rain_count_1_to_5",
case when hour_prec_history.hour_count_since_precip between 5 and 10 then 1 else 0 end as "since_rain_count_5_to_10",
case when hour_prec_history.hour_count_since_precip between 10 and 20 then 1 else 0 end as "since_rain_count_10_to_20",
case when hour_prec_history.hour_count_since_precip between 20 and 24 then 1 else 0 end as "since_rain_count_20_to_24",
case when hour_prec_history.hour_count_since_precip between 24 and 30 then 1 else 0 end as "since_rain_count_24_to_30",
case when hour_prec_history.hour_count_since_precip between 30 and 36 then 1 else 0 end as "since_rain_count_30_to_36",
case when hour_prec_history.hour_count_since_precip between 37 and 47 then 1 else 0 end as "since_rain_count_37_to_47",
case when hour_prec_history.hour_count_since_precip between 48 and 57 then 1 else 0 end as "since_rain_count_48_to_57",
case when hour_prec_history.hour_count_since_precip between 58 and 65 then 1 else 0 end as "since_rain_count_58_to_65",
case when hour_prec_history.hour_count_since_precip between 66 and 72 then 1 else 0 end as "since_rain_count_66_to_72",
case when hour_prec_history.hour_count_since_precip between 73 and 96 then 1 else 0 end as "since_rain_count_73_to_96",
case when hour_prec_history.hour_count_since_precip between 97 and 120 then 1 else 0 end as "since_rain_count_97_to_120",
case when hour_prec_history.hour_count_since_precip between 121 and 144 then 1 else 0 end as "since_rain_count_121_to_144",
case when hour_prec_history.hour_count_since_precip between 145 and 168 then 1 else 0 end as "since_rain_count_145_to_168",
case when hour_prec_history.hour_count_since_precip >= 168 then 1 else 0 end as "since_rain_count_>168"  
FROM weather_import
join weather_deltas on weather_deltas.hr=weather_import.hr join hour_prec_history on hour_prec_history.hr=weather_import.hr;
