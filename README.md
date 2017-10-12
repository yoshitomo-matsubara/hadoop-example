# Hadoop Example
My private Maven project with Apache Hadoop examples to help you understand how its MapReduce framework works.  
You can run example codes on your (local) computer instead of Hadoop clusters.  
Supporting material: [SlideShare](http://www.slideshare.net/yoshitomo-matsubara/understanding-hadoop-through-examples)  
Spark ver. is also available [here](https://github.com/yoshitomo-matsubara/spark-example).

## Requirement
- Java 1.8+
- hadoop-common (2.7.3)
- hadoop-client (2.7.3)

## Examples
### Quiz grading
**ymatsubara.hadoop.example.grading.GradingDriver** grades 5 quizzes (max 10pt for each) for 500 students.

---
### Word count
**ymatsubara.hadoop.example.wordcount.WordCountDriver** counts words in English articles (space-delimited text files).  
