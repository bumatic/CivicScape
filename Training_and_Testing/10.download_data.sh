#!/bin/bash -e
code=$WC_TRAIN/10.Code
work=$WC_JOB_DIR/10.work
output=$WC_JOB_DIR/10.download_data

#CITY_ST_weather () {
#OUTPUT_FILENAME=$output/weather.csv
#lat={here}
#lng={here}
#Job_Dir=$output

#export OUTPUT_FILENAME=$WC_JOB_DIR/weather.csv
#export lat={here}
#export lng={here}
#export Job_Dir=$output
#export city=$WC_CITY

####local  - 
####python $CODE_SHARED/get_forecasts.py credshere $city $OUTPUT_FILENAME $lat $lng
#python $code/get_forecasts.py $city $OUTPUT_FILENAME $lat $lng $Job_Dir
#}


philadelphia_pa_weather () {
OUTPUT_FILENAME=$WC_JOB_DIR/weather.csv
lat=39.9526 
lng=-75.1652
Job_Dir=$output

export OUTPUT_FILENAME=$WC_JOB_DIR/weather.csv
export lat=39.9526 
export lng=-75.1652
export Job_Dir=$WC_JOB_DIR
export city=$WC_CITY

#local  - 
#python $CODE_SHARED/get_forecasts.py credshere $city $OUTPUT_FILENAME $lat $lng
python $code/get_forecasts.py $city $OUTPUT_FILENAME $lat $lng $Job_Dir $FORECAST_IO_KEY

}

chicago_il_weather () {
OUTPUT_FILENAME=$output/weather.csv
lat=41.8781
lng=-87.6298
Job_Dir=$output

export OUTPUT_FILENAME=$WC_JOB_DIR/weather.csv
export lat=41.8781 
export lng=-87.6298
export Job_Dir=$output
export city=$WC_CITY

#local  - 
#python $CODE_SHARED/get_forecasts.py credshere $city $OUTPUT_FILENAME $lat $lng
python $code/get_forecasts.py $city $OUTPUT_FILENAME $lat $lng $Job_Dir $FORECAST_IO_KEY
}

philadelphia_pa_crime() {
	crime_csv=$output/philadelphia_crime.csv 
	echo "Downloading crimes to $crime_csv"
	curl -o $crime_csv "https://data.phila.gov/resource/sspu-uyfa.json"	
}

philadelphia_pa_census() {
	#zip=$work/tracts.zip
	#wget https://www.opendataphilly.org/dataset/census-tracts -O $zip
	#unzip $zip -d $work
	shapefile="$work/Census_Tracts_2010.shp"
	sql=$output/census_tracts4326.sql
	shp2pgsql -s 4326 $shapefile census_tracts > $sql

	echo "CREATE INDEX ix_censustracts_geom ON census_tracts using GIST(geom); " >> $sql

}
philadelphia_pa_311() {
	311_csv=$output/philadelphia_311.csv 
	echo "Downloading crimes to $crime_csv"
	curl -o $311_csv "https://data.phila.gov/resource/4t9v-rppq.json"	
}



if [ $WC_CITY = "philadelphia_pa" ]; then
	philadelphia_pa_crime
	philadelphia_pa_weather
	philadelphia_pa_311
else
	"$WC_CITY unimplemented"
	exit 1
fi

