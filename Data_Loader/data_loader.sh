
WC_CITY=$1
WC_DATA_TYPE=$2
WC_PG_PASSWORD=$3

usage() {
	echo "Invalid set of parameters"
	echo "Usage: ./data_loader.sh $CITY $DATA_TYPE"
	echo "eg:    ./data_loader.sh seattle_wa crime"
}

if [ "x" == "x$WC_CITY" ] || [ "x" == "x$WC_DATA_TYPE" ] ; then
	usage
	exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export CODE=$DIR/Code/$WC_CITY
export CODE_SHARED=$DIR/Code/Shared

export WC_JOB_DIR=$DIR/jobs/$WC_CITY/$WC_DATA_TYPE
export WC_CITY=$WC_CITY

mkdir -p $WC_JOB_DIR
#rm -f $WC_JOB_DIR/*


# PG aws vars
export sql_cmd="psql -h $WC_PG_HOST -U master $WC_CITY"


# Run the appropriate script if it exists- otherwise error out
script=$CODE/${WC_CITY}_$WC_DATA_TYPE.sh
if [ -e $script ]; then
	$script "$3" "$4"
else
	echo "could not find the script to run this task:"
	echo $script
	exit 1
fi



