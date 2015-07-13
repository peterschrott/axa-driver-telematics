#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Illegal number of arguments."
    echo "Usage: "
    echo "  run-od-ext <sdtWeight> <L1> <L1>"
    exit 1
fi

echo "starting outlier detectin with parameter: stdWeight=$1; L1=$2; L2=$3"

/share/spark-1.3.1-bin-hadoop2.4/bin/spark-submit --master spark://ibm-power-1.dima.tu-berlin.de:7070 --num-executors 9 --driver-memory 16g --executor-memory 16g --executor-cores 1 /home/aim3-team2/outlier-detection/apps/outlier-detection-1.0-SNAPSHOT.jar hdfs:///user/aim3-team2/drives/res-feature/ ext hdfs:///user/aim3-team2/drives/res-kaggle-ext-$1-$2-$3 $1 $2 $3

/share/spark-1.3.1-bin-hadoop2.4/bin/spark-submit --master spark://ibm-power-1.dima.tu-berlin.de:7077 --num-executors 9 --driver-memory 32g --executor-memory 32g --executor-cores 1 /home/aim3-team2/outlier-detection/apps/outlier-detection-1.0_2-SNAPSHOT.jar hdfs:///user/aim3-team2/drives/res-feature/ ext1 hdfs:///user/aim3-team2/drives/res-kaggle-ext-1.0-0.0-0.1 1.0 0.0 0.1

## $HOME/outlier-detection/tools/spark-1.4.0-bin-hadoop2.4/bin/spark-submit --master yarn-client --num-executors 9 --driver-memory 4g --executor-memory 2g --executor-cores 1 $HOME/outlier-detection//apps/outlier-detection-1.0-SNAPSHOT.jar hdfs:///user/aim3-team2/drives/res-feature/ ext hdfs:///user/aim3-team2/drives/res-kaggle-ext $1 $2 $3

