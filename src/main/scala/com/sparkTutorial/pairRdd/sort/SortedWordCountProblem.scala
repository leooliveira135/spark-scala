package com.sparkTutorial.pairRdd.sort

import com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice.AvgCount
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("sortedWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val words = sc.textFile("in/word_count.text")
    val wordsRDD = words.flatMap(line => line.split(" "))
    val wordsPairRDD = wordsRDD.map(word => (word,1))

    val wordCounts = wordsPairRDD.reduceByKey((x,y) => x + y)
    /*val countWordCounts = wordCounts.map(wordCount => (wordCount._2, wordCount._1))
    val sortedWordCounts = countWordCounts.sortByKey(ascending = false)*/
    val sortedWordCounts = wordCounts.sortBy(wordCount => wordCount._2, ascending = false)

    for((word, count) <- sortedWordCounts.collect()) println(word + " : " + count)

  }

}

