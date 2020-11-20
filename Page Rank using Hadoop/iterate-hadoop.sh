#!/bin/sh
CONVERGE=1
rm -rf v*
rm -rf log*

bin/hadoop dfsadmin -safemode leave

bin/hadoop fs dfs -rm -r skipTrash /output*

bin/hadoop jar share/hadoop/tools/lib/hadoop-*streaming*.jar \
-mapper "BDAssignment2/mapper1.py " \
-reducer "BDAssignment2/reducer1.py 'v'"  \
-input /wordCountDataset.txt.gz \
-output /output1 #has adjacency list


while [ "$CONVERGE" -ne 0 ]
do
        bin/hadoop jar share/hadoop/tools/lib/hadoop-*streaming*.jar \
        -mapper "BDAssignment2/mapper2.py 'v' " \
        -reducer "BDAssignment2/reducer2.py" \
        -input /output1 \
        -output /output2
        touch v1
        bin/hadoop fs -cat /output2/* > v1
        CONVERGE=$(python3 check_conv.py >&1)
        hdfs dfs -rm -r /output2
        echo $CONVERGE

done
