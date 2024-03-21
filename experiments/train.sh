for alpha in 0.1; do 
    for gamma in 0.7; do
        echo "alpha $alpha gamma $gamma"
        stdout_file="eval_logs/stdout_alpha${alpha}_gamma${gamma}.log"
        stderr_file="eval_logs/stderr_alpha${alpha}_gamma${gamma}.log"
        # Ensure the train_logs directory exists
        mkdir -p eval_logs
        # Execute the command and use tee for stdout and stderr
        ( docker exec experiments-pod-1 python pod/train.py --gamma $gamma --alpha $alpha 2> >(tee "$stderr_file" >&2) | tee "$stdout_file" ) &
    done
done
echo "done with script"
