SPARK_HOME=/home/hadoop/hadoop_nfs/spark-1.5.2-bin-hadoop2.6
$SPARK_HOME/bin/spark-submit  ./wet_test.py > log.log

#$SPARK_HOME/bin/spark-submit --master local --num-executors 4 ../src/wet_LDA.py
