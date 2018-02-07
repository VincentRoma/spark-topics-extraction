## How it works

1. Configure HDFS input files path in config.properties (Only Parquet for now)

2. Configure input text column in config.properties

3. Configure HDFS output path in config.properties

4. `spark-submit --class org.opentools.extraction.ExtractTopics --master yarn --deploy-mode cluster ExtractTopics-1.0.jar`

## Compilation

```
mvn clean compile
```

Uber Jar
```
mvn compile assembly:single
```


## References

* [Spark APP + mvn + Intellij](http://knowdimension.com/en/data/create-a-spark-application-with-scala-using-maven-on-intellij/)

* [Spark text tokenization](https://community.hortonworks.com/articles/84781/spark-text-analytics-uncovering-data-driven-topics.html)

* [Spark MLLib Clustering](https://spark.apache.org/docs/1.6.1/mllib-clustering.html)

* [Spark ML StopWordsRemover](https://spark.apache.org/docs/1.6.0/ml-features.html#stopwordsremover)
