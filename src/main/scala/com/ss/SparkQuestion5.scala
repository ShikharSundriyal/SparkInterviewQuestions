package com.ss

import org.apache.spark.sql.SparkSession
//PROBLEM Statement : Find the lowest salary in each department
object SparkQuestion5 {
  def main(args: Array[String]): Unit = {
    // Find the lowest salary per department
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val sparkSession = SparkSession.builder().master("local[*]").appName("Interview Questions").getOrCreate()
    sparkSession.sparkContext.setLogLevel("error")

    import sparkSession.implicits._
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.row_number
    import org.apache.spark.sql.functions._
    val df=Seq((1,100),(1,200),(2,300),(2,50),(2,50)).toDF("id","amount")
    val parti=Window.partitionBy($"amount")
    df.withColumn("rank",dense_rank().over(Window.partitionBy($"id").orderBy($"amount".desc)))
    df.createOrReplaceTempView("test")
    sparkSession.sql("select * from (select id ,amount , dense_rank() over(partition by id order by amount) as drank from test) where drank=1").show()
  }
}
