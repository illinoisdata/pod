#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -eq 0 ]
then
    suts=("${SUTS[@]}")
elif [ "$#" -eq 1 ]
then
    IFS=',' read -ra suts <<< $1
else
    echo "Invalid number of arguments (expected <= 1), $# provided"
    echo 'Example: bash scripts/bench_exp2.sh'
    echo 'Example: bash scripts/bench_exp2.sh snp,pfl'
    exit 1
fi
echo "Running experiments/bench_exp1_nb.sh with rmtree variants on [ ${suts[*]} ]"

tv_list=(
    "1e0"
    "1e1"
    "1e2"
    "1e3"
)
tl_list=(
    "1e3"
    "1e4"
    "1e5"
    "1e6"
)
tml_list=(
    "1"
    "10"
    "100"
)
tme_list=(
    "1"
    "10"
    "100"
)

for ((i = 0; i < ${#suts[@]}; i++)) do
    for ((tvi = 0; tvi < ${#tv_list[@]}; tvi++)) do
        for ((tli = 0; tli < ${#tl_list[@]}; tli++)) do
            for ((tmli = 0; tmli < ${#tml_list[@]}; tmli++)) do
                for ((tmei = 0; tmei < ${#tme_list[@]}; tmei++)) do
                    nb_key="rmtree[tv=${tv_list[$tvi]}][tl=${tl_list[$tvi]}][tml=${tml_list[$tvi]}][tme=${tme_list[$tvi]}]"
                    bash experiments/bench_exp1_nb.sh ${suts[$i]} ${nb_keys[$j]} ${nb_key}
                done
            done
        done
    done
done
