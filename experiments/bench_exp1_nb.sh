#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT
source experiments/bench_consts.sh

if [ "$#" -ne 2 ]
then
    echo "Require 2 argument (SUT, NBNAME), $# provided"
    echo 'Example: bash scripts/bench_exp1_nb.sh snp storesfg'
    echo 'Example: bash scripts/bench_exp1_nb.sh pfl rmlist'
    exit 1
fi

SUT=$1
NBNAME=$2
get_sut_args ${SUT} sut_args
get_nb_path ${NBNAME} nbkey nbpath nbargs

echo "Using SUT=${SUT}, NBNAME=${NBNAME}"
echo "      sut_args=\"${sut_args}\""
echo "      nbkey=${nbkey}, nbpath=${nbpath}, nbargs=\"${nbargs}\""
sleep 1

prepare_sut ${SUT}
python pod/bench.py exp1 \
    --expname exp1_${SUT}_${NBNAME} \
    --nbname ${nbkey} \
    --nb ${nbpath} \
    ${nbargs} \
    ${sut_args}
