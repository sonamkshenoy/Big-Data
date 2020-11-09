from __future__ import print_function

import sys

from pyspark.sql import SparkSession
if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: task1.py <word> <dataset1> <dataset2>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
            .builder\
            .appName("Task 1")\
            .getOrCreate()

    givenWord = sys.argv[1]

    lines_dataset2 = spark.read.option("header", True).csv(sys.argv[3])


    filtered_dataset_recognized = lines_dataset2[(lines_dataset2['word']==givenWord) & (lines_dataset2['recognized']==True)]
    filtered_dataset_unrecognized = lines_dataset2[(lines_dataset2['word']==givenWord) & (lines_dataset2['recognized']==False)]

    recognized_avg = filtered_dataset_recognized.agg({"Total_Strokes":"avg"}).collect()
    recognized_dict = {}

    for i in recognized_avg:
        recognized_dict.update(i.asDict())

    unrecognized_avg = filtered_dataset_unrecognized.agg({"Total_Strokes":"avg"}).collect()
    unrecognized_dict = {}

    for i in unrecognized_avg:
        unrecognized_dict.update(i.asDict())

    if(recognized_dict['avg(Total_Strokes)']):
        print("%0.5f"%(recognized_dict['avg(Total_Strokes)']))
    else:
        print("%0.5f"%0)

    if(unrecognized_dict['avg(Total_Strokes)']):
        print("%0.5f"%(unrecognized_dict['avg(Total_Strokes)']))
    else:
        print("%0.5f"%0)

    spark.stop()

