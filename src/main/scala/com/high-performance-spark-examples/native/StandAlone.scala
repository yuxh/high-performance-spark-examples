package com.highperformancespark.examples.ffi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object StandAlone {
  def main(args: Array[String]) {
    println("fuck")
    //    val partitionedrdd = parRdd.partitionBy(new HashPartitioner(3))
    //    val otherRdd = partitionRdd.mapValues(value =>value+ 1)
    //    val patoinedRdd = partitionedRdd.join(otherRdd)
    val words = Array("one")
//    val wordsPairRdd = sc.parallelize(words)
    //      .map(w => (w, 1))
    //tag::systemLoadLibrary[]
    //    System.loadLibrary("highPerformanceSpark0")s
    //end::systemLoadLibrary[]
    //    println(new SumJNI().sum(Array(1,2,3)))
//    val spark: SparkSession =
//    SparkSession
//      .builder()
//      .appName("Time Usage")
//      .config("spark.master", "local")
//      .getOrCreate()

    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    val sc = spark.sparkContext
    val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)


  }
}
