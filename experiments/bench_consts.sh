#/!bin/bash

POD_DIR=/tmp/pod

############
## NB_MAP ##
############

NB_MAP=(
    ## Synthetic notebooks

    "rmlist rmlist"
    "rmtree rmtree"

    ## Jupyter notebooks

    "nbreadon notebooks/nb_readon.ipynb"
    "nbupdate notebooks/nb_update.ipynb"
    "nbappend notebooks/nb_append.ipynb"

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

    "agripred notebooks/agricultural-drought-prediction.ipynb"
    "ecomsmph notebooks/e-comm-data-mining-for-smartphone-products.ipynb"
    "amexdata notebooks/amex-dataset.ipynb"
    "msciedaw notebooks/msci-eda-which-makes-sense.ipynb"

    ## Python scripts.

    "srmlist notebooks/script_rmlist.py"
    "pystone notebooks/pystone.py"
    "nbody notebooks/nbody.py"
    "spnorm notebooks/spectralnorm.py"

    "netmnist notebooks/torch_mnist.py"
    "rlactcri notebooks/torch_actor_critic.py"
    "vaenet notebooks/torch_vae.py"
    "siamese notebooks/torch_siamese_network.py"
    "wordlang notebooks/torch_word_language_model.py"
    "tseqpred notebooks/torch_time_sequence_prediction.py"
    # "langtlsl notebooks/torch_language_translation.py"
)

# Parse into two arrays.
NB_KEYS=()
NB_PATHS=()
for ((i = 0; i < ${#NB_MAP[@]}; i++)) do
    IFS=' ' read -ra nb_map_elems <<< "${NB_MAP[$i]}"
    NB_KEYS[i]+=${nb_map_elems[0]}
    NB_PATHS[i]+=${nb_map_elems[1]}
done


function get_nb_args() {
    local key=$1
    local val=$2
    local RET_NBARGS=$3
    if [[ $key == "exc" ]]
    then
        nb_args="--exclude_save_names"
    elif [[ $key == "tc" ]]
    then
        nb_args="--rmtree_num_cells ${val}"
    elif [[ $key == "tv" ]]
    then
        nb_args="--rmtree_var_size ${val}"
    elif [[ $key == "tl" ]]
    then
        nb_args="--rmtree_list_size ${val}"
    elif [[ $key == "tml" ]]
    then
        nb_args="--rmtree_percent_list_mutate ${val}"
    elif [[ $key == "tme" ]]
    then
        nb_args="--rmtree_percent_elem_mutate ${val}"
    elif [[ $key == "n" ]]
    then
        nb_args=""
    else
        echo "ERROR (get_nb_args): Unknown key= ${key}, with value= ${value}"
        exit 1
    fi
    eval $RET_NBARGS="'${nb_args}'"
    return 0
}

# The mapping function.
function get_nb_path() {
    local nbname=$1
    local RET_NBKEY=$2
    local RET_NBPATH=$3
    local RET_NBARGS=$4

    # Notebook key is everything before `[`.
    ret_nbkey="${nbname%%\[*}"
    pairs="${nbname#*$ret_nbkey}"

    # Parse optional flags in each pair of `[`` and `]`.
    ret_nbargs=""
    while [[ "$pairs" =~ \[([^\]]*)\](.*) ]]; do
        pair="${BASH_REMATCH[1]}"
        pairs="${BASH_REMATCH[2]}"

        # Parse `key` or `key=value`.
        if [[ "$pair" == *"="* ]]; then
            # Split the pair into key and value
            IFS='=' read -r key value <<< "$pair"
        else
            # If no '=', treat the key as the entire pair and value as empty
            key="$pair"
            value=""
        fi

        # Map the short name to full argument format.
        get_nb_args "${key}" "${value}" new_nbargs
        ret_nbargs="${ret_nbargs} ${new_nbargs}"
    done

    for i in ${!NB_KEYS[@]}; do
        if [ "${NB_KEYS[$i]}" = "$ret_nbkey" ]; then
            eval $RET_NBKEY="'${ret_nbkey}'"
            eval $RET_NBPATH="'${NB_PATHS[$i]}'"
            eval $RET_NBARGS="'${ret_nbargs}'"
            return 0
        fi
    done
    echo "ERROR: Invalid nb_key $ret_nbkey from [ ${NB_KEYS[*]} ]"
    exit 1
}

##########
## SUTS ##
##########

SUTS=(
    "snp" 
    "snz" 
    "snx" 
    "dill" 
    "cpkl" # Recursion error on self-references
    "shev" 
    "zodb" 
    "zosp" 
    "criu" 
    "crii" 
    "noop" 
    "exhaust" 
    "imm"
    "pfl"
    "pfa"
    "pgl"
    "pga"
    "pgaz"
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
    "pgnoop"
    "pflc"
    "prcc"
)

function get_sut_args() {
    local _SUT=$1
    local retVal=$2
    if [[ $_SUT == "snp" ]]
    then
        sut_args="--sut snapshot --pod_dir ${POD_DIR}"
    elif [[ $_SUT == "snz" ]]
    then
        sut_args="--sut snapshotzlib --pod_dir ${POD_DIR}"
    elif [[ $_SUT == "snx" ]]
    then
        sut_args="--sut snapshotxdelta --pod_dir ${POD_DIR}"
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
    elif [[ $_SUT == "noop" ]]
    then
        sut_args="--sut noop"
    elif [[ $_SUT == "exhaust" ]]
    then
        sut_args="--sut exhaust --pod_dir ${POD_DIR}"
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
    elif [[ $_SUT == "pgaz" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --podding_model greedy-lgb --roc_path models/roc_lgb.txt --sut_async --sut_compress"
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
    elif [[ $_SUT == "pgnoop" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --sut_async --pod_noop"
    elif [[ $_SUT == "pflc" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --enable_feature --podding_model manual-collect --pod_active_filter False --auto_static_checker always --always_lock_all True"
    elif [[ $_SUT == "prcc" ]]
    then
        sut_args="--sut pod_file --pod_dir ${POD_DIR} --enable_feature --podding_model roc-collect --pod_active_filter False --auto_static_checker always --always_lock_all True"
    else
        echo "ERROR (get_sut_args): Invalid SUT $_SUT from [ ${SUTS[*]} ]"
        exit 1
    fi
    eval $retVal="'${sut_args}'"
    return 0
}

function prepare_sut() {
    local _SUT=$1
    if [[ $_SUT == "snp" || $_SUT == "snz" || $_SUT == "snx" ]]
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
    elif [[ $_SUT == "noop" ]]
    then
        :
    elif [[ $_SUT == "exhaust" ]]
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
    elif [[ $_SUT == "pgaz" ]]
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
    elif [[ $_SUT == "pgnoop" ]]
    then
        rm -r ${POD_DIR}
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
