package com.ravi.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.sql.Date

object Task2Diff {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task2Diff").setMaster("local")
    val sc = new SparkContext(conf)

    val dataset1: DataFrame = sc.textFile("/Users/rshekh2/Desktop/user_score/UserScore.txt").map(_.split(","))
                               .map(attributes => UserScore(attributes(0).toLong, Date.valueOf(attributes(1)), attributes(2).toInt))
                               .toDF()
    
    val dataset2: DataFrame = sc.textFile("/Users/rshekh2/Desktop/user_score/UserScore1.txt").map(_.split(","))
                               .map(attributes => UserScore(attributes(0).toLong, Date.valueOf(attributes(1)), attributes(2).toInt))
                               .toDF()
                               
    dataset1.printSchema()
    dataset2.printSchema()
    dataset1.show(20)
    dataset2.show(20)

    val output = findDelta(dataset1, dataset2, Seq("user_id", "date"))
    output.printSchema()
    output.show(20)
  }

  def findDelta(dataset1: DataFrame, dataset2: DataFrame, cond: Seq[String]): DataFrame = {
    val joinDf = dataset1.join(dataset2, Seq("user_id"), "fullouter").select(col("user_id"), dataset1.col("date"), dataset1.col("score"), dataset2.col("date").alias("date1"), dataset2.col("score").alias("score1"))
    val deltaDf = joinDf.withColumn("delta_type", when(col("date").isNull, "deleted")
                        .when(col("date1").isNull, "inserted")
                        .when((col("date").isNotNull && col("date1").isNotNull && col("date").notEqual(col("date1"))) 
                            || (col("score").isNotNull && col("score1").isNotNull && col("score").notEqual(col("score1"))),
                            "updated").otherwise(""))
                        .select(col("user_id"),col("date"),col("score"), col("delta_type")) 
    return deltaDf
  }
}



