package com.ravi.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.sql.Date

case class UserScore(user_id: Long, date: java.sql.Date, score: Int)

object MarkMostRecent {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MarkMostRecent").setMaster("local")
    val sc = new SparkContext(conf)

    val dataset: DataFrame = sc.textFile("/Users/rshekh2/Desktop/user_score/UserScore.txt").map(_.split(","))
                               .map(attributes => UserScore(attributes(0).toLong, Date.valueOf(attributes(1)), attributes(2).toInt))
                               .toDF()
    dataset.printSchema()
    dataset.show(20)

    val result = markMostRecent(dataset)
    result.printSchema()
    result.show(20)
  }

  def markMostRecent(dataset: DataFrame): DataFrame = {
    val partitionWindow = Window.partitionBy(col("user_id")).orderBy(col("date").desc, col("score").desc)
    val rankDf = dataset.withColumn("rank", rank().over(partitionWindow))
    val markedDf = rankDf.withColumn("status", when(col("rank") === 1, "most_recent").otherwise(""))
                         .select(col("user_id"), col("date"), col("score"), col("status"))
    return markedDf
  }
}



