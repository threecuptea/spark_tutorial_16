package org.freemind.spark.basic

import org.apache.spark.{SparkConf, SparkContext}
/**
First Created by dev on 12/30/15.

  */

object MyWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length < 2)
      throw new IllegalArgumentException()

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val input = sc.textFile(args(0))
    val outputDir = args(1)

    val stopWords = Array("","a","an","the","this","to","for","and","##","can","on","is","in","of","also","if","with","you","or")
    val punct = "\"'`,:.![]<>-"
    val words = input.flatMap(line => line.split(" ")).cache()
    //clean up punctuation as well as stop words.   It won't be as fine as well Lucene/sort because it does not solve for ex, the difference between example and examples
    val cleanedWords = words.map(w => w.dropWhile(punct.contains(_)).reverse.dropWhile(punct.contains(_)).reverse).filter(!stopWords.contains(_))

    val wc = cleanedWords.map(w => (w,1)).reduceByKey( _+_ )
    //Print the top 5 with most counted.
    wc.top(10)(Ordering[Int].on(_._2)).foreach(println)

    wc.saveAsTextFile(outputDir)

  }

}
