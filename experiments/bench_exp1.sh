#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT
source experiments/bench_consts.sh

if [ "$#" -eq 0 ]
then
    suts=("${SUTS[@]}")
    nb_keys=("${NB_KEYS[@]}")
elif [ "$#" -eq 1 ]
then
    IFS=',' read -ra suts <<< $1
    nb_keys=("${NB_KEYS[@]}")
elif [ "$#" -eq 2 ]
then
    IFS=',' read -ra suts <<< $1
    IFS=',' read -ra nb_keys <<< $2
else
    echo "Invalid number of arguments (expected <= 2), $# provided"
    echo 'Example: bash scripts/bench_exp1.sh'
    echo 'Example: bash scripts/bench_exp1.sh snp,pfl'
    echo 'Example: bash scripts/bench_exp1.sh snp,pfl rmlist,storesfg'
    exit 1
fi
echo "Running experiments/bench_exp1_nb.sh on [ ${suts[*]} ] x [ ${nb_keys[*]} ]"

for ((i = 0; i < ${#suts[@]}; i++)) do
    for ((j = 0; j < ${#nb_keys[@]}; j++)) do
        bash experiments/bench_exp1_nb.sh ${suts[$i]} ${nb_keys[$j]}
    done
done
