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
    "twittnet notebooks/twitter_networks.ipynb"
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
    "dill" 
    "cpkl" # Recursion error on self-references
    "shev" 
    "zodb" 
    "zosp" 
    "criu" 
    "crii" 
    "imm"
    "pfl"
    "pfa"
    "pgl"
    "pga"
    "pgcache0"
    "pgcache4"
    "pgcache16"
    "pgcache64"
    "pgcache256"
    "pgcache1024"
    "pgcache4096"
    "pgcache16384"
    "pgcache65536"
    "pgcache262144"
    "pgcache1048576"
    "pgcache1m"
    "pgcache2m"
    "pgcache4m"
    "pgcache8m"
    "pgcache15m"
    "pgcache31m"
    "pgcache62m"
    "pgcache125m"
    "pgcache250m"
    "pgcache500m"
    "pgcache1g"
    "pgcache2g"
    "pgcache4g"
    "pgcache8g"
    "pgcache16g"
    "pgcache10g"
    "pgcache100g"
    "pgnoavf"
    "pgcache0noavf"
    "pglnostatic"
    "pgnostatic"
    "pgnoavl"
    "pgnoavlstatic"
    "pglnoavlstatic"
    "pg0"
    "pg1"
    "prand"
    "pnv"
    "pnb"
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
    elif [[ $_SUT == "dill" ]]
    then
        sut_args="--sut dill --pod_dir ${POD_DIR}"
    elif [[ $_SUT == "cpkl" ]]
    then
        sut_args="--sut cloudpickle --pod_dir ${POD_DIR}"
    elif [[ $_SUT == "shev" ]]
    then
        sut_args="--sut shelve --pod_dir ${POD_DIR}"
    elif [[ $_SUT == "zodb" ]]
    then
        sut_args="--sut zodb --pod_dir ${POD_DIR}"
    elif [[ $_SUT == "zosp" ]]
    then
        sut_args="--sut zosp --pod_dir ${POD_DIR}"
    elif [[ $_SUT == "criu" ]]
    then
        sut_args="--sut criu --pod_dir ${POD_DIR}"
    elif [[ $_SUT == "crii" ]]
    then
        sut_args="--sut crii --pod_dir ${POD_DIR}"
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
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt"
    elif [[ $_SUT == "pga" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async"
    elif [[ $_SUT == "pgcache0" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 0"
    elif [[ $_SUT == "pgcache4" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 4"
    elif [[ $_SUT == "pgcache16" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 16"
    elif [[ $_SUT == "pgcache64" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 64"
    elif [[ $_SUT == "pgcache256" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 256"
    elif [[ $_SUT == "pgcache1024" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 1024"
    elif [[ $_SUT == "pgcache4096" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 4096"
    elif [[ $_SUT == "pgcache16384" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 16384"
    elif [[ $_SUT == "pgcache65536" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 65536"
    elif [[ $_SUT == "pgcache262144" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 262144"
    elif [[ $_SUT == "pgcache1048576" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 1048576"
    elif [[ $_SUT == "pgcache1m" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 1000000"
    elif [[ $_SUT == "pgcache2m" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 1953125"
    elif [[ $_SUT == "pgcache4m" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 3906250"
    elif [[ $_SUT == "pgcache8m" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 7812500"
    elif [[ $_SUT == "pgcache15m" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 15625000"
    elif [[ $_SUT == "pgcache31m" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 31250000"
    elif [[ $_SUT == "pgcache62m" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 62500000"
    elif [[ $_SUT == "pgcache125m" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 125000000"
    elif [[ $_SUT == "pgcache250m" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 250000000"
    elif [[ $_SUT == "pgcache500m" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 500000000"
    elif [[ $_SUT == "pgcache1g" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 1000000000"
    elif [[ $_SUT == "pgcache2g" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 2000000000"
    elif [[ $_SUT == "pgcache4g" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 4000000000"
    elif [[ $_SUT == "pgcache8g" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 8000000000"
    elif [[ $_SUT == "pgcache10g" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 10000000000"
    elif [[ $_SUT == "pgcache16g" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 16000000000"
    elif [[ $_SUT == "pgcache100g" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_cache_size 100000000000"
    elif [[ $_SUT == "pgnoavf" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_active_filter False"
    elif [[ $_SUT == "pgcache0noavf" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --pod_active_filter False  --pod_cache_size 0"
    elif [[ $_SUT == "pgnostatic" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --auto_static_checker always"
    elif [[ $_SUT == "pgnoavl" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --always_lock_all True"
    elif [[ $_SUT == "pgnoavlstatic" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --auto_static_checker always --always_lock_all True"
    elif [[ $_SUT == "pglnoavlstatic" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --auto_static_checker always --always_lock_all True"
    elif [[ $_SUT == "pglnostatic" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --auto_static_checker always"
    elif [[ $_SUT == "pg0" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-const --const_roc 0.0"
    elif [[ $_SUT == "pg1" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-const --const_roc 1.0"
    elif [[ $_SUT == "prand" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model random --sut_async"
    elif [[ $_SUT == "pnv" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model naive --sut_async"
    elif [[ $_SUT == "pnb" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model naive-bundle --sut_async"
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
    elif [[ $_SUT == "dill" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "cpkl" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "shev" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "zodb" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "zosp" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "criu" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "crii" ]]
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
    elif [[ $_SUT == "pgcache0" || $_SUT == "pgcache4" || $_SUT == "pgcache16" || $_SUT == "pgcache64" || $_SUT == "pgcache256" || $_SUT == "pgcache1024" || $_SUT == "pgcache4096" || $_SUT == "pgcache16384" || $_SUT == "pgcache65536" || $_SUT == "pgcache262144" || $_SUT == "pgcache1048576" || $_SUT == "pgcache1m" || $_SUT == "pgcache2m" || $_SUT == "pgcache4m" || $_SUT == "pgcache8m" || $_SUT == "pgcache15m" || $_SUT == "pgcache31m" || $_SUT == "pgcache62m" || $_SUT == "pgcache125m" || $_SUT == "pgcache250m" || $_SUT == "pgcache500m" || $_SUT == "pgcache1g" || $_SUT == "pgcache2g" || $_SUT == "pgcache4g" || $_SUT == "pgcache8g" || $_SUT == "pgcache10g" || $_SUT == "pgcache16g" || $_SUT == "pgcache100g" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "pgnoavf" || $_SUT == "pgcache0noavf" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "pglnostatic" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "pgnostatic" || $_SUT == "pgnoavl" || $_SUT == "pgnoavlstatic" || $_SUT == "pglnoavlstatic" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "pg0" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "pg1" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "prand" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "pnv" ]]
    then
        rm -r ${POD_DIR}
    elif [[ $_SUT == "pnb" ]]
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
