package com.faurecia.stratos.tools

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf,SparkContext}

object ExtractTopics {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Stratos Topic Extraction")
    val sc = new SparkContext(conf)

    /*
    def cleanup_text(record):
    text  = record[8]
    uid   = record[9]
    words = text.split()

    # Default list of Stopwords
      stopwords_core = ['a', u'about', u'above', u'after', u'again', u'against', u'all', u'am', u'an', u'and', u'any', u'are', u'arent', u'as', u'at',
    u'be', u'because', u'been', u'before', u'being', u'below', u'between', u'both', u'but', u'by',
    u'can', 'cant', 'come', u'could', 'couldnt',
    u'd', u'did', u'didn', u'do', u'does', u'doesnt', u'doing', u'dont', u'down', u'during',
    u'each',
    u'few', 'finally', u'for', u'from', u'further',
    u'had', u'hadnt', u'has', u'hasnt', u'have', u'havent', u'having', u'he', u'her', u'here', u'hers', u'herself', u'him', u'himself', u'his', u'how',
    u'i', u'if', u'in', u'into', u'is', u'isnt', u'it', u'its', u'itself',
    u'just',
    u'll',
    u'm', u'me', u'might', u'more', u'most', u'must', u'my', u'myself',
    u'no', u'nor', u'not', u'now',
    u'o', u'of', u'off', u'on', u'once', u'only', u'or', u'other', u'our', u'ours', u'ourselves', u'out', u'over', u'own',
    u'r', u're',
    u's', 'said', u'same', u'she', u'should', u'shouldnt', u'so', u'some', u'such',
    u't', u'than', u'that', 'thats', u'the', u'their', u'theirs', u'them', u'themselves', u'then', u'there', u'these', u'they', u'this', u'those', u'through', u'to', u'too',
    u'under', u'until', u'up',
    u'very',
    u'was', u'wasnt', u'we', u'were', u'werent', u'what', u'when', u'where', u'which', u'while', u'who', u'whom', u'why', u'will', u'with', u'wont', u'would',
    u'y', u'you', u'your', u'yours', u'yourself', u'yourselves']

    # Custom List of Stopwords - Add your own here
      stopwords_custom = ['']
    stopwords = stopwords_core + stopwords_custom
    stopwords = [word.lower() for word in stopwords]

    text_out = [re.sub('[^a-zA-Z0-9]','',word) for word in words]                                       # Remove special characters
    text_out = [word.lower() for word in text_out if len(word)>2 and word.lower() not in stopwords]     # Remove stopwords and words under X length
    return text_out
    */

  }
}