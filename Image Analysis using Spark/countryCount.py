#!/usr/bin/python3
from __future__ import print_function

import sys

from pyspark.sql import SparkSession

if __name__ == "__main__":

    if len(sys.argv) < 5:
        print("Usage: task1.py <word> <stroke> <dataset1> <dataset2>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
            .builder\
            .appName("Task 2")\
            .getOrCreate()

    givenWord = sys.argv[1]
    maxStrokes = int(sys.argv[2])
  

    lines_dataset1 = spark.read.option("header", True).csv(sys.argv[3])
    lines_dataset2 = spark.read.option("header", True).csv(sys.argv[4])

    combined_dataset = lines_dataset1.join(lines_dataset2, lines_dataset1.key_id == lines_dataset2.key_id).drop(lines_dataset1.word)

    filtered_dataset_unrecognized = combined_dataset[(combined_dataset["word"] == givenWord) & (combined_dataset['recognized']==False)]

    unrecognized_avg = filtered_dataset_unrecognized.agg({"Total_Strokes":"sum"}).collect()
    unrecognized_dict = {}

    for i in unrecognized_avg:
        unrecognized_dict.update(i.asDict())

    if(unrecognized_dict['sum(Total_Strokes)'] and unrecognized_dict['sum(Total_Strokes)'] < maxStrokes):
        count = filtered_dataset_unrecognized.groupby('countrycode').agg({'countrycode':'count'}).collect()


        countryCount = {}
        for i in count:
            countryCount[i.asDict()['countrycode']] = i.asDict()['count(countrycode)']

        finalCount = {k:v for k,v in sorted(countryCount.items(), key=lambda item: item[0])}

        for i in finalCount:
            print(i, finalCount[i], sep=",") # Also with 5 decimals?


    else:
        # What should we do?
        print("Reached here?")
        pass

    spark.stop()
