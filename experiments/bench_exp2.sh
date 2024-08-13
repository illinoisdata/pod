#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 6 ]
then
    echo "Invalid number of arguments (expected 6), $# provided"
    echo 'Example: bash scripts/bench_exp2.sh pga 10 1e0,1e1,1e2,1e3 1e3,1e4,1e5,1e6 1,10,100 1,10,100'
    exit 1
fi

IFS=',' read -ra suts <<< $1
IFS=',' read -ra tc_list <<< $2
IFS=',' read -ra tv_list <<< $3
IFS=',' read -ra tl_list <<< $4
IFS=',' read -ra tml_list <<< $5
IFS=',' read -ra tme_list <<< $6

echo "Running experiments/bench_exp1_nb.sh on [ ${suts[*]} ] x [ tc= ${tc_list[*]} ] x [ tv= ${tv_list[*]} ] x [ tl= ${tl_list[*]} ] x [ tml= ${tml_list[*]} ] x [ tme= ${tme_list[*]} ]"

for ((i = 0; i < ${#suts[@]}; i++)) do
    for ((tci = 0; tci < ${#tc_list[@]}; tci++)) do
        for ((tvi = 0; tvi < ${#tv_list[@]}; tvi++)) do
            for ((tli = 0; tli < ${#tl_list[@]}; tli++)) do
                for ((tmli = 0; tmli < ${#tml_list[@]}; tmli++)) do
                    for ((tmei = 0; tmei < ${#tme_list[@]}; tmei++)) do
                        nb_key="rmtree[exc][tc=${tc_list[$tci]}][tv=${tv_list[$tvi]}][tl=${tl_list[$tvi]}][tml=${tml_list[$tvi]}][tme=${tme_list[$tvi]}]"
                        bash experiments/bench_exp1_nb.sh ${suts[$i]} ${nb_keys[$j]} ${nb_key}
                    done
                done
            done
        done
    done
done
