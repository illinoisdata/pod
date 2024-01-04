#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 9 ]
then
  echo "Require 9 argument (BLOB_ROOT, KEYSET_ROOT, DB_ROOT, INDEX_BUILDER, INDEX_DRAFTERS, ACTION, REPEAT, RESET_SCRIPT, STORAGE), $# provided"
  echo 'Example: bash scripts/sosd_experiment.sh file://$(pwd)/../SOSD/data file://$(pwd)/../SOSD/keyset file://$(pwd)/tmp/btree btree btree build 1 ~/reload_nfs.sh nfs'
  echo 'Example: bash scripts/sosd_experiment.sh file://$(pwd)/../SOSD/data file://$(pwd)/../SOSD/keyset file://$(pwd)/tmp/enb_stb enb step,band_greedy,band_equal build 1 ~/reload_nfs.sh nfs'
  exit 1
fi

BLOB_ROOT=$1
KEYSET_ROOT=$2
DB_ROOT=$3
INDEX_BUILDER=$4
INDEX_DRAFTERS=$5
ACTION=$6
REPEAT=$7
RESET_SCRIPT=$8
STORAGE=$9
