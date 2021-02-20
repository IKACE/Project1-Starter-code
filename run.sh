# echo "start Amazon Review 20M with k=2 s=30" > log.txt
# date >> log.txt
# hadoop jar build/libs/FrequentItemsets.jar -k 2 -s 30  --ratingsFile Ama20TF  --outputScheme Ama20R
# echo "Finished job at" >> log.txt
# date >> log.txt
# echo "---------" >> log.txt
# hdfs dfs -get Ama20R1
# hdfs dfs -get Ama20R2

echo "start Amazon Review 40M with k=2 s=30" >> log.txt
date >> log.txt
hadoop jar build/libs/FrequentItemsets.jar -k 2 -s 30  --ratingsFile Ama40TF  --outputScheme Ama40R
echo "Finished job at" >> log.txt
date >> log.txt
echo "---------" >> log.txt
hdfs dfs -get Ama40R1
hdfs dfs -get Ama40R2

echo "start Amazon Review 60M with k=2 s=30" >> log.txt
date >> log.txt
hadoop jar build/libs/FrequentItemsets.jar -k 2 -s 30  --ratingsFile Ama60TF  --outputScheme Ama60R
echo "Finished job at" >> log.txt
date >> log.txt
echo "---------" >> log.txt
hdfs dfs -get Ama60R1
hdfs dfs -get Ama60R2

echo "start Amazon Review 60M with k=2 s=45 for 60M" >> log.txt
date >> log.txt
hadoop jar build/libs/FrequentItemsets.jar -k 2 -s 45  --ratingsFile Ama60TF  --outputScheme Ama60s45R
echo "Finished job at" >> log.txt
date >> log.txt
echo "---------" >> log.txt
hdfs dfs -get Ama60s45R1
hdfs dfs -get Ama60s45R2

echo "start Amazon Review 60M with k=2 s=60 for 60M" >> log.txt
date >> log.txt
hadoop jar build/libs/FrequentItemsets.jar -k 2 -s 60  --ratingsFile Ama60TF  --outputScheme Ama60s60R
echo "Finished job at" >> log.txt
date >> log.txt
echo "---------" >> log.txt
hdfs dfs -get Ama60s60R1
hdfs dfs -get Ama60s60R2
