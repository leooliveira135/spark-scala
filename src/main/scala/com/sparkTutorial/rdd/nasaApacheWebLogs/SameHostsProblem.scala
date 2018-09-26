package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.spark.{SparkConf, SparkContext}

object SameHostsProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
       Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

       Example output:
       vagrant.vf.mmc.com
       www-a1.proxy.aol.com
       .....

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */

    val conf = new SparkConf().setAppName("sameHosts").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val julyFirstLogs = sc.textFile(path = "in/nasa_19950701.tsv")
    val augustFirstLogs = sc.textFile(path = "in/nasa_19950801.tsv")

    val julyFirstHosts = julyFirstLogs.map(line => line.split("\t")(0))
    val augustFirstHosts = augustFirstLogs.map(line => line.split("\t")(0))

    val intersection = julyFirstHosts.intersection(augustFirstHosts)

    val sample = intersection.filter(host => host != "host")

    sample.saveAsTextFile(path = "out/nasa_logs_same_hosts.csv")
  }
}
