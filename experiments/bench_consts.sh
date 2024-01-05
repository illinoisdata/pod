#/!bin/bash

############
## NB_MAP ##
############

NB_MAP=(
    "rmlist rmlist"

    # "storesfg notebooks/store-sales-ts-forecasting-a-comprehensive-guide.ipynb"
    # "itsttime notebooks/it-s-that-time-of-the-year-again.ipynb"
    # "buildats notebooks/building-an-asset-trading-strategy.ipynb"
    # "spacestt notebooks/spaceship-titanic-a-complete-guide.ipynb"
    # "betterxb notebooks/better-xgb-baseline.ipynb"
    # "tpsmay22 notebooks/tpsmay22-insightful-eda-fe-baseline.ipynb"
)

# Parse into two arrays.
NB_KEYS=()
NB_PATHS=()
for ((i = 0; i < ${#NB_MAP[@]}; i++)) do
    IFS=' ' read -ra nb_map_elems <<< "${NB_MAP[$i]}"
    NB_KEYS[i]+=${nb_map_elems[0]}
    NB_PATHS[i]+=${nb_map_elems[1]}
done

# The mapping function.
function get_nb_path() {
    local _NBNAME=$1
    local retVal=$2
    for i in ${!NB_KEYS[@]}; do
        if [ "${NB_KEYS[$i]}" = "$_NBNAME" ]; then
            eval $retVal="'${NB_PATHS[$i]}'"
            return 0
        fi
    done
    echo "ERROR: Invalid NBNAME $_NBNAME"
    exit 1
}

##########
## SUTS ##
##########

SUTS=(
    "snp" 
    "imm"
    "pfl"
)

function get_sut_args() {
    local _SUT=$1
    local retVal=$2
    if [[ $_SUT == "snp" ]]
    then
        sut_args="--sut snapshot --pod_dir ${POD_DIR}"
    elif [[ $_SUT == "imm" ]]
    then
        sut_args="--sut inmem_dict"
    elif [[ $_SUT == "pfl" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR}"
    else
        echo "ERROR: Invalid SUT $_SUT"
        exit 1
    fi
    eval $retVal="'${sut_args}'"
    return 0
}

function prepare_sut() {
    local _SUT=$1
    if [[ $_SUT == "snp" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "imm" ]]
    then
        :
    elif [[ $_SUT == "pfl" ]]
    then
        rm -r ${POD_DIR}
    else
        echo "ERROR: Invalid SUT $_SUT"
        exit 1
    fi
    return 0
}
