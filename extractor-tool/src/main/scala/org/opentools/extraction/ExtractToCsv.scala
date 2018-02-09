package org.opentools.extraction

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf,SparkContext}

class ExtractToCsv {
    def main(args: Array[String]){
        // Spark conf & set up
        val conf = new SparkConf().setMaster("yarn").setAppName("Data Selection / Extraction / Conversion")
        val sc = new SparkContext(conf)

        // Load user specific properties
        val prop = new Properties()
        prop.load(new FileInputStream("config.properties"))
        val folderpath = prop.getProperty("hdfs.folder.path")
        val folderoutput = prop.getProperty("hdfs.folder.output")
        val timefield = prop.getProperty("dataset.time.field")
        val timewindow = prop.getProperty("dataset.time.window")

        //TODO Correctly split undefined number of filter properties
        val fields = prop.getProperty("dataset.fields.filter.name").split(",")
        val fields_values = prop.getProperty("dataset.fields.filter.value").split(",")

        //TODO check format of input timefield and timewindow to avoid job failure

        val df = sqlContext.read.parquet(folderpath)

        val df_timed = df.filter(df(timefield) > timewindow)
        val df_filtered = df_timed.filter(fields=fields_values) //FIXME

        df_filtered.write.format("com.databricks.spark.csv").option("header", "true").save(folderoutput)
    }
}
