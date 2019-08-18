package com.ss
import org.apache.spark.sql._

//Problem Statement : Get all the records from left table which are not present in right table
object SparkQuestion3 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val sparkSession = SparkSession.builder().master("local[*]").appName("Interview Questions").getOrCreate()
    import sparkSession.implicits._
    sparkSession.sparkContext.setLogLevel("error")

    val left = Seq((0, "zero"), (1, "one")).toDF("id", "left")
    val right = Seq((0, "zero"), (2, "two"), (3, "three")).toDF("id", "right")
    left.join(right,Seq("id"),"left_anti").show
  }

}
