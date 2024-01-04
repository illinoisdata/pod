TMP_DIR=/tmp/pod

python pod/bench.py exp1 --expname test_imm --nb rmlist --sut inmem_dict

rm -r ${TMP_DIR}
python pod/bench.py exp1 --expname test_pfl --nb rmlist --sut pod_file --pod_dir ${TMP_DIR}

rm -r ${TMP_DIR}
python pod/bench.py exp1 --expname test_snp --nb rmlist --sut snapshot --pod_dir ${TMP_DIR}

python pod/plot.py exp1 --result_paths result/test_imm result/test_pfl result/test_snp/
