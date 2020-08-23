package com.highperformancespark.examples.goldilocks

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.collection.immutable.IndexedSeq

object IterativeGoldilocksPractice {
  def findRankStatistics(dataFrame: DataFrame, ranks: List[Long]): Map[Int, Iterable[Double]] = {
    require(ranks.forall(_ > 0))
    val numberOfColumns = dataFrame.schema.length
    var i               = 0
    var result          = Map[Int, Iterable[Double]]()
    while (i < numberOfColumns) {
      val col                            = dataFrame.rdd.map(row => row.getDouble(i))
      val sortedCol: RDD[(Double, Long)] = col.sortBy(v => v).zipWithIndex()
      val ranksOnly: RDD[Double]         = sortedCol.filter(tuple => ranks.contains(tuple._2 + 1)).keys
      val list: Array[Double]            = ranksOnly.collect()

      result += (i -> list)
      i += 1
    }
    result
  }
  def findRankStatisticsGroupBy(dataFrame: DataFrame, ranks: List[Long]): Map[Int, Iterable[Double]] = {
    require(ranks.forall(_ > 0))
    val pairRDD = Utils.mapToKeyValuePairs(dataFrame)
    val groupColumns: RDD[(Int, Iterable[Double])] = pairRDD
      .groupByKey()
    groupColumns

  }

}

object Utils {
  //返回 （列号，值）
  def mapToKeyValuePairs(dataFrame: DataFrame): RDD[(Int, Double)] = {

    val numberOfColumns = dataFrame.schema.length

    dataFrame.rdd.flatMap { row =>
      Range(0, numberOfColumns).map(colIndex => (colIndex, row.getDouble(colIndex)))
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("MnMCount")
      .getOrCreate()
    val sparkContext = spark.sparkContext
//    val sqlContext   = new SQLContext(sc)
    val testRanks = List(3L, 8L)

    val (smallTestData, result) =
      DataCreationUtils.createLocalTestData(5, 10, testRanks)
    val schema = StructType(result.keys.toSeq.map(n => StructField("Column" + n.toString, DoubleType)))
    val smallTestDF: DataFrame =
      spark.createDataFrame(sparkContext.makeRDD(smallTestData), schema)

    val iterative =
      IterativeGoldilocksPractice.findRankStatistics(smallTestDF, testRanks)
    val groupByKey =
      GoldilocksGroupByKey.findRankStatistics(smallTestDF, testRanks)
  }
}

object DataCreationUtils {
  def createLocalTestData(
      numberCols: Int,
      numberOfRows: Int,
      targetRanks: List[Long]
  ): (IndexedSeq[Row], Map[Int, Iterable[Long]]) = {

    val cols     = Range(0, numberCols).toArray
    val scalers  = cols.map(x => 1.0)
    val rowRange = Range(0, numberOfRows)
    val columnArray: Array[IndexedSeq[Double]] = cols.map(columnIndex => {
      val columnValues = rowRange.map(x => (Math.random(), x)).sortBy(_._1).map(_._2 * scalers(columnIndex))
      columnValues
    })
    val rows = rowRange.map(rowIndex => {
      Row.fromSeq(cols.map(colI => columnArray(colI)(rowIndex)).toSeq)
    })

    val result: Map[Int, Iterable[Long]] = cols
      .map(i => {
        (i, targetRanks.map(r => Math.round((r - 1) / scalers(i))))
      })
      .toMap

    (rows, result)
  }

  def createDistributedData(
      sc: SparkContext,
      partitions: Int,
      elementsPerPartition: Int,
      numberOfColumns: Int
  ): RDD[(Long, List[Int])] = {
    val partitionsStart: RDD[Int] = sc.parallelize(Array.fill(partitions)(1))
    partitionsStart.repartition(partitions)

    var data: RDD[(Long, List[Int])] = partitionsStart
      .mapPartitionsWithIndex {
        case (partIndex, elements) =>
          val rows = Range(0, elementsPerPartition)
            .map(x => (Math.random(), x))
            .map {
              case ((randomNumber, rowValue)) =>
                (
                  randomNumber,
                  //index of element
                  (
                    partIndex * elementsPerPartition.toLong + rowValue,
                    List(rowValue + partIndex * elementsPerPartition)
                  )
                )
            }
          rows.toIterator
      }
      .sortByKey()
      .values

    Range(0, numberOfColumns).foreach(x => {
      val nextColumn: RDD[(Long, Int)] = partitionsStart
        .mapPartitionsWithIndex {
          case (partIndex, elements) =>
            val rows = Range(0, elementsPerPartition)
              .map(x => (Math.random(), x))
              .map {
                case ((randomNumber, rowValue)) =>
                  (
                    randomNumber,
                    //index of element
                    (partIndex * elementsPerPartition.toLong + rowValue, rowValue + partIndex * elementsPerPartition)
                  )
              }
            rows.toIterator
        }
        .sortByKey()
        .values

      data = nextColumn.join(data).mapValues(x => x._1 :: x._2)
    })
    data
  }
}
._1 :: x._2)
    })
    data
  }
}
