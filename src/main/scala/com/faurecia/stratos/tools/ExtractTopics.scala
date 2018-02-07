package org.opentools.text.extraction

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.feature.StopWordsRemover

class ExtractTopics {
    def main(args: Array[String]){
        // Spark conf & set up
        val conf = new SparkConf().setAppName("Topic Extraction")
        val sc = new SparkContext(conf)

        // Load user specific properties
        val prop = new Properties()
        prop.load(new FileInputStream("config.properties"))
        val filepath = prop.getProperty("hdfs.folder.path")
        val fileouput = prop.getProperty("hdfs.folder.output")
        val textcolumn = prop.getProperty("dataset.textcolumn")


        val df = sqlContext.read.parquet(filepath)

        val tokenizer = new Tokenizer().setInputCol(textcolumn).setOutputCol("words")
        val regexTokenizer = new RegexTokenizer()
        .setInputCol(textcolumn)
        .setOutputCol("words")
        .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

        val tokenized = tokenizer.transform(df)
        tokenized.select("words", "label").take(3).foreach(println)
        val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
        regexTokenized.select("words", "label").take(3).foreach(println)

        val remover = new StopWordsRemover()
        .setInputCol("words")
        .setOutputCol("filtered")

        remover.transform(tokenized).show()
    }
}
