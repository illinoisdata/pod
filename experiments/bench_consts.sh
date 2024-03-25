#/!bin/bash

POD_DIR=/tmp/pod

############
## NB_MAP ##
############

NB_MAP=(
    "rmlist rmlist"

    "storesfg notebooks/store-sales-ts-forecasting-a-comprehensive-guide.ipynb"
    "itsttime notebooks/it-s-that-time-of-the-year-again.ipynb"
    "buildats notebooks/building-an-asset-trading-strategy.ipynb"
    # "spacestt notebooks/spaceship-titanic-a-complete-guide.ipynb"
    "betterxb notebooks/better-xgb-baseline.ipynb"
    "tpsmay22 notebooks/tpsmay22-insightful-eda-fe-baseline.ipynb"
    "denoisdw notebooks/denoising-with-direct-wavelet-transform.ipynb"
    "denoisft notebooks/fast-fourier-transform-denoising.ipynb"
    "ai4code notebooks/ai4code-detailed-eda.ipynb"
    "covid193 notebooks/cv19w3-2-v2-play-2-v3fix-sub-last6dayopt.ipynb"
    # "twittnet notebooks/twitter_networks.ipynb"
    "skltweet notebooks/sklearn_tweet_classification.ipynb"
    # "trainlms notebooks/04_training_linear_models.ipynb"

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
    echo "ERROR: Invalid NBNAME $_NBNAME from [ ${NB_KEYS[*]} ]"
    exit 1
}

##########
## SUTS ##
##########

SUTS=(
    "snp" 
    "imm"
    "pfl"
    "pfa"
    "pgl"
    "pga"
    "pnv"
    "pna"
    "ppg"
    "prd"
    "pnj"
    "pmg"
    "pflc"
    "prcc"

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
    elif [[ $_SUT == "pfa" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --sut_async"
    elif [[ $_SUT == "pgl" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy --roc_path models/roc_model.pkl"
    elif [[ $_SUT == "pga" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy --roc_path models/roc_model.pkl --sut_async"
    elif [[ $_SUT == "pnv" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model naive"
    elif [[ $_SUT == "pna" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model naive --sut_async"
    elif [[ $_SUT == "ppg" ]]
    then
        sut_args="--sut pod_psql --psql_hostname podpsql --psql_port 5432"
    elif [[ $_SUT == "prd" ]]
    then
        sut_args="--sut pod_redis --redis_hostname podredis --redis_port 6379"
    elif [[ $_SUT == "pnj" ]]
    then
        sut_args="--sut pod_neo4j --neo4j_uri neo4j://podneo4j --neo4j_port 7687 --neo4j_password podneo4jPassword --neo4j_database pod"
    elif [[ $_SUT == "pmg" ]]
    then
        sut_args="--sut pod_mongo --mongo_hostname podmongo --mongo_port 27017"
    elif [[ $_SUT == "pflc" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --enable_feature --podding_model manual-collect"
    elif [[ $_SUT == "prcc" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --enable_feature --podding_model roc-collect"
    else
        echo "ERROR (get_sut_args): Invalid SUT $_SUT from [ ${SUTS[*]} ]"
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
    elif [[ $_SUT == "pfa" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "pgl" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "pga" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "pnv" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "pna" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "ppg" ]]
    then
        export PGPASSWORD=postgres
        echo "psql \"DROP DATABASE IF EXISTS pod;\""
        psql --host=podpsql --port=5432 \
            -U postgres -d postgres \
            -c "DROP DATABASE IF EXISTS pod;"
    elif [[ $_SUT == "prd" ]]
    then
        echo "redis-cli flushall"
        redis-cli -h podredis -p 6379 flushall
    elif [[ $_SUT == "pnj" ]]
    then
        echo "cypher-shell \"CREATE OR REPLACE DATABASE pod;\""
        cypher-shell -a neo4j://podneo4j:7687 \
            -u neo4j -p podneo4jPassword \
            -d system \
            "CREATE OR REPLACE DATABASE pod;"
    elif [[ $_SUT == "pmg" ]]
    then
        echo "mongo \"db.dropDatabase();\""
        mongosh pod --host podmongo --port 27017 --eval "db.dropDatabase();"
    elif [[ $_SUT == "pflc" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "prcc" ]]
    then
        rm -r ${POD_DIR}
    else
        echo "ERROR (prepare_sut): Invalid SUT $_SUT from [ ${SUTS[*]} ]"
        exit 1
    fi
    return 0
}
