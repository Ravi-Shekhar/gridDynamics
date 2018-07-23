# gridDynamics

Task 1 -> 
********************
val dataset: DataFrame = sc.textFile("/Users/rshekh2/Desktop/user_score/UserScore.txt").map(_.split(","))
                               .map(attributes => UserScore(attributes(0).toLong, Date.valueOf(attributes(1)), attributes(2).toInt))
                               .toDF()
                               
dataset.printSchema()
dataset.show(20)

val result = markMostRecent(dataset)
result.printSchema()
result.show(20)

def markMostRecent(dataset: DataFrame): DataFrame = {
    val partitionWindow = Window.partitionBy(col("user_id")).orderBy(col("date").desc, col("score").desc)
    val rankDf = dataset.withColumn("rank", rank().over(partitionWindow))
    val markedDf = rankDf.withColumn("status", when(col("rank") === 1, "most_recent").otherwise(""))
                         .select(col("user_id"), col("date"), col("score"), col("status"))
    return markedDf
  }
  
 Task 2 -> 
********************
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
