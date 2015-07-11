#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Illegal number of arguments."
    echo "Usage: "
    echo "  run-od-ext <sdtWeight> <L1> <L1>"
    exit 1
fi

echo "starting outlier detectin with parameter: stdWeight=$1; L1=$2; L2=$3"

$HOME/outlier-detection/tools/spark-1.4.0-bin-hadoop2.4/bin/spark-submit --master yarn-client --num-executors 9 --driver-memory 4g --executor-memory 2g --executor-cores 1 $HOME/outlier-detection//apps/outlier-detection-1.0-SNAPSHOT.jar hdfs:///user/aim3-team2/drives/res-feature/ ext hdfs:///user/aim3-team2/drives/res-kaggle-ext $1 $2 $3
