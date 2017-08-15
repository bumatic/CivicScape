#!/bin/bash -e
code=$WC_TRAIN/90.Code
shared_code=$WC_TRAIN/SharedCode
bin_work=$WC_JOB_DIR/40.work
bin_tracts=$WC_JOB_DIR/30.database_export
work=$WC_JOB_DIR/90.work
output=$WC_JOB_DIR/90.test_results
#work=$WC_TRAIN_DATA/90.work
#output=$WC_TRAIN_DATA/90.output

bin_script=$WC_TRAIN/40.Code/bin.sh
testing_csv_dir=$WC_JOB_DIR/30.database_export
training_csv_dir=$WC_JOB_DIR/40.prepared_csvs
model_dir=$WC_JOB_DIR/70.models



echo "$testing_csv"

mkdir -p $code $work $output

testing_csv="$testing_csv_dir/${WC_CITY}_test_data.csv"

testing_file="$work/${WC_CITY}_test_data_binned.csv"

##to do: deprecate this step or replace ; for now skip binning scripts and move to testing
cp $testing_csv $testing_file 





