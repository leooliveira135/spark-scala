package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */

    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("sumOfNumbers").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val prime_numbers = sc.textFile("in/prime_nums.text")
    val numberRDD = prime_numbers.flatMap(line => line.split("\\s+"))

    val validNumbers = numberRDD.filter(numberRDD => !numberRDD.isEmpty)
    val convertNumbers = validNumbers.map(numberRDD => numberRDD.toInt)

    val numbers = convertNumbers.take(10)
    for (number <- numbers) println(number)

    println("Soma dos primeiros números = " +  numbers.reduce((x,y) => x+y))
    println("Soma Total = " + convertNumbers.reduce((x,y) => x + y))

  }
}
