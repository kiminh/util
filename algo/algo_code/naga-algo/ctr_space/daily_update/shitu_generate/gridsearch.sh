#!/bash/bin

alpha_param=(0.03 0.04 0.05 0.06 0.07 0.08 0.09 0.1)
beta_param=(1)
for alpha in ${alpha_param[*]};do
    for beta in ${beta_param[*]};do
        ./bin/ftrl train.ins.shuf l1_reg=1 l2_reg=0 alpha=$alpha beta=$beta
        ./bin/ftrl test.ins task=pred model_in=lr_model.dat
        echo $alpha $beta
        python3 bin/metric.py pred.txt 
    done
done
