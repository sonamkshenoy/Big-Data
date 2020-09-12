# Big-Data  
Repository for Big Data Projects

## Map Reduce using Hadoop (in Java)   
The analysis was made on the Quickdraw Dataset. This dataset consists of detailed description of each drawing in an NDJSON (Newline Delimited JSON) format.

### Task 1:  Object Instances   
---
* Sub Task 1 :  Records are filtered based on these criteria, which if satisfied are called clean records:
  * word: Should contain alphabets ans whitespaces only
  * countrycode: Should contain two uppercase letters only   
  * recognized: Should be a boolean value i.e. either true or false  
  * key_id: Should be a numeric string containing 16 characters only  
* Sub Task 2 : Count of words that belong to the cleaned dataset and that are recognized.  
* Sub Task 3 : Count of words from the cleaned records that are not recognized but fall on a weekend.  

Cleaning of the dataset as per the criteria is done using REGEX during the mapping stage.  

Arguments that are to be passed in the command line:   
  i. The word whose count is to be found and on which further processing is later done.  



### Task 2:   Object Instances by Country    
---
* Sub Task 1  : Records are to be considered only if the Euclidean distance between the 0th coordinates of the first stroke (stroke refers to the "drawing" array in the dataset) and the origin is greater than a specific distance. This distance is passed in the command line.   
* Sub Task 2  : Count the number of occurrences of a specific word **per** country in the cleaned dataset.   
  
  
Arguments that are to be passed in the command line:    
  i. The word whose count is to be found and on which further processing is later done.  
  ii. The minimum distance based on which records are filtered (as mentioned above).



### Running the Project  
1. Download JSON-Java from https://github.com/stleary/JSON-java. Add this library to Hadoop Classpath. This library will be used for extracting and parsing JSON.    
```
export HADOOP_CLASSPATH="$JAVA_HOME/lib/tools.jar:json-java.jar"
```
2. Compile the Java file    
```
bin/hadoop com.sun.tools.javac.Main <path to java file>
```
3. Create a jar   
```
jar cf <name of jar>.jar <name of java file>*.class
```
4. Place the NDJSON file in the HDFS input folder
```
bin/hdfs dfs -mkdir -p input
bin/hdfs dfs -put input/plane_carriers.ndjson input
```
5. Execute   
For Task 1:  
`bin/hadoop jar <path to jar file> <class name> /user/ubuntu/input/plane_carriers.ndjson user/ubuntu/output <word>`
OR   
For Task 2:  
`bin/hadoop jar <path to jar file> CountCountriesPart2 /user/ubuntu/input/plane_carriers.ndjson user/ubuntu/output <word> <distance>`   
Example: word = airplane and distance = 100   

6. Finally, the output can be seen as:  
```
bin/hdfs dfs -cat user/ubuntu/output/part-r-00000
```
