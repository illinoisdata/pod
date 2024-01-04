#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT
source experiments/bench_consts.sh

POD_DIR=/tmp/pod

if [ "$#" -ne 2 ]
then
    echo "Require 2 argument (SUT, NBNAME), $# provided"
    echo 'Example: bash scripts/bench_exp1_nb.sh snp storesfg'
    echo 'Example: bash scripts/bench_exp1_nb.sh pfl rmlist'
    exit 1
fi

SUT=$1
NBNAME=$2
get_sut_args $SUT sut_args
get_nb_path $NBNAME nbpath

echo "Using SUT=${SUT}, NBNAME=${NBNAME}"
echo "      sut_args=\"${sut_args}\""
echo "      nbpath=${nbpath}"
sleep 1

rm -r ${POD_DIR}
python pod/bench.py exp1 \
    --expname exp1_${SUT}_${NBNAME} \
    --nb ${nbpath} \
    ${sut_args}
