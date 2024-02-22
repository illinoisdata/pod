for alpha in 0.1 0.2 0.3 0.4 0.5;
do 
    for gamma in 0.6 0.7 0.8 0.9 1.0;
        do
            echo "alpha $alpha gamma $gamma"
            ( docker exec experiments-pod-1 python pod/train.py --gamma $gamma --alpha $alpha & )
        done
echo "done with script"
done;