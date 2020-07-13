package com

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark
import org.scalatest.FunSuite

class MyTestPractice extends FunSuite with SharedSparkContext {
  private lazy val myCollection =
    "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
  private lazy val words             = sc.parallelize(myCollection, 2)
  private lazy val chars             = words.flatMap(word => word.toLowerCase.toSeq)
  private lazy val KVcharacters      = chars.map(letter => (letter, 1))
  def maxFunc(left: Int, right: Int) = math.max(left, right)
  def addFunc(left: Int, right: Int) = left + right

  test("understand aggregate") {
    def addFunc(left: Int, right: Int) = left + right
    val myCollection =
      "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val words        = sc.parallelize(myCollection, 2)
    val chars        = words.flatMap(word => word.toLowerCase.toSeq)
    val KVcharacters = chars.map(letter => (letter, 1))

    val kvCharactersByGroup = KVcharacters
      .groupByKey()
    kvCharactersByGroup.foreach(println)

    kvCharactersByGroup
      .map(row => (row._1, row._2.reduce(addFunc)))
      .collect()

  }

  test("combineByKey ") {
    val studentRDD = sc.parallelize(
      Array(
        ("Joseph", "Maths", 83),
        ("Joseph", "Physics", 74),
        ("Joseph", "Chemistry", 91),
        ("Joseph", "Biology", 82),
        ("Jimmy", "Maths", 69),
        ("Jimmy", "Physics", 62),
        ("Jimmy", "Chemistry", 97),
        ("Jimmy", "Biology", 80),
        ("Tina", "Maths", 78),
        ("Tina", "Physics", 73),
        ("Tina", "Chemistry", 68),
        ("Tina", "Biology", 87),
        ("Thomas", "Maths", 87),
        ("Thomas", "Physics", 93),
        ("Thomas", "Chemistry", 91),
        ("Thomas", "Biology", 74),
        ("Cory", "Maths", 56),
        ("Cory", "Physics", 65),
        ("Cory", "Chemistry", 71),
        ("Cory", "Biology", 68),
        ("Jackeline", "Maths", 86),
        ("Jackeline", "Physics", 62),
        ("Jackeline", "Chemistry", 75),
        ("Jackeline", "Biology", 83),
        ("Juan", "Maths", 63),
        ("Juan", "Physics", 69),
        ("Juan", "Chemistry", 64),
        ("Juan", "Biology", 60)
      ),
      3
    )
    def createCombiner = (tuple: (String, Int)) => (tuple._2.toDouble, 1)

    def mergeValue =
      (accumulator: (Double, Int), element: (String, Int)) => (accumulator._1 + element._2, accumulator._2 + 1)

    def mergeCombiner =
      (accumulator1: (Double, Int), accumulator2: (Double, Int)) =>
        (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)

    val combRDD = studentRDD
      .map(t => (t._1, (t._2, t._3)))
      .combineByKey(createCombiner, mergeValue, mergeCombiner)
      .map(e => (e._1, e._2._1 / e._2._2))
  }

  test("aggregate ") {
    val keysWithValuesList = Array(
      "foo=A",
      "foo=A",
      "foo=A",
      "foo=A",
      "foo=B",
      "bar=C",
      "bar=D",
      "bar=D"
    )

    val data = sc.parallelize(keysWithValuesList)
    //Create key value pairs
    val kv                 = data.map(_.split("=")).map(v => (v(0), v(1))).cache()
    val initialCount       = 0;
    val addToCounts        = (n: Int, v: String) => n + 1
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2
    val countByKey =
      kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)

    countByKey.foreach(println)

    val data2 = sc.parallelize(List(6, 2, 3, 4, 5, 1), 3)
    data2.mapPartitionsWithIndex(printPartitionInfo).collect
    def interPartition(a: Int, b: Int): Int = { a * b }
    def crossPartition(a: Int, b: Int): Int = { a + b }
    def printPartitionInfo(index: Int, iter: Iterator[(Int)]): Iterator[Int] = {
      println("---------------[partID:" + index + ", val: " + iter.toList + "]")
      iter
    }
    val result = data2.aggregate(10)(interPartition, crossPartition)
    print(result)
  }

  test("create tuple") {
    val tup = ("programming scala", 2014)
    val t1  = (1, "two")
    val v2  = 1 -> 2
    val v3  = 1  → 2
    Map("one" -> 1, "two" -> 2)
    Array(1, 3).toIterator

    val iterator = Iterator(1, 2, 3, 4, 5, 6, 7)
    var sum      = 0
    while (iterator.hasNext) {
      sum += iterator.next
    }
    println("sum is " + sum)
    val expression = if (iterator.isEmpty) "iterator is empty" else "iterator is not empty"
    println(expression)
  }

}
