package com.ravi.spark

import org.scalatest.junit.AssertionsForJUnit
import org.apache.spark.SparkContext
import org.junit.Test
import org.junit.Before
import org.apache.spark.SparkConf
import main.scala.com.matthewrathbone.spark.ExampleJob
import org.junit.After

class SparkJoinsTest extends AssertionsForJUnit {

  var sc: SparkContext = _
  
  @Before
  def initialize() {
    val conf = new SparkConf().setAppName("MarkMostRecent").setMaster("local")
    sc = new SparkContext(conf)
  }
  
  @After
  def tearDown() {
    sc.stop()
  }
  
  @Test
  def testMarkMostRecentCode() {
    val job = new MarkMostRecent()
    val result = job.markMostRecent()
    assert(result.collect()(0)._1 === "5")
    assert(result.collect()(0)._2 === "2018-05-17")
    assert(result.collect()(0)._3 === "100")
    assert(result.collect()(0)._4 === "most_recent")
  }
}