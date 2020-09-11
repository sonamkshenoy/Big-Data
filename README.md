# Big-Data  
Repository for Big Data Projects

## Map Reduce using Hadoop (in Java)   
The analysis was made on the Quickdraw Dataset. This dataset consists of detailed description of each drawing.

### Task 1:  Object Instances  
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
* Sub Task 1  : Records are to be considered only if the Euclidean distance between the 0th coordinates of the first stroke (stroke refers to the "drawing" array in the dataset) and the origin is greater than a specific distance. This distance is passed in the command line.   
* Sub Task 2  : Count the number of occurrences of a specific word **per** country in the cleaned dataset.   
  
  
Arguments that are to be passed in the command line:    
  i. The word whose count is to be found and on which further processing is later done.  
  ii. The minimum distance based on which records are filtered (as mentioned above).
