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
elif [ "$#" -eq 1 ]
then
    IFS=',' read -ra suts <<< $1
else
    echo "Invalid number of arguments, $# provided"
    echo 'Example: bash scripts/bench_exp1.sh'
    echo 'Example: bash scripts/bench_exp1.sh snp,pfl'
    exit 1
fi
echo "Running experiments/bench_exp1_nb,sh on [ ${suts[*]} ]"
sleep 2

for ((i = 0; i < ${#suts[@]}; i++)) do
    for ((j = 0; j < ${#NB_KEYS[@]}; j++)) do
        bash experiments/bench_exp1_nb.sh ${suts[$i]} ${NB_KEYS[$j]}
    done
done
