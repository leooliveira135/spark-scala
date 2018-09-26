package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */

    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val airports = sc.textFile(path = "in/airports.text")
    val airportsByLatitude = airports.filter(
        line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)

    val airportsAndLatitude = airportsByLatitude.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + "," + splits(6)
    })
    airportsAndLatitude.saveAsTextFile(path = "out/airports_and_latitude.text")
  }
}
