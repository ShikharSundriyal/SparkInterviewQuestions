package com.ss

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
//count the number of people residing in each city, there are 2 tables employee(empid,name,citycode) and city(citycode,cityname)
object SparkQuestion2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val sparkSession = SparkSession.builder().master("local[*]").appName("Interview Questions").getOrCreate()
    import sparkSession.implicits._
    sparkSession.sparkContext.setLogLevel("error")
    val empdf = Seq((1, "a", 1), (2, "b", 2), (3, "c", 1), (4, "d", 1), (5, "e", 3)).toDF("empid", "empname", "empcitycode")
    val citydf = Seq((1, "delhi"), (2, "punjab"), (3, "mumbai")).toDF("CityCode", "CityName")
    val joineddf = empdf.join(citydf, $"empcitycode" === $"CityCode", "inner")
    //DataframesWay
    val window = Window.partitionBy($"empcitycode")
    val result = joineddf.withColumn("citywiseresult", count($"empid") over window).select("CityName", "citywiseresult").distinct.show

    //SPARK SQL WAY
    joineddf.createOrReplaceTempView("resu")
    val res = sparkSession.sql("select distinct CityName, count(empid) over (partition by empcitycode) as citywiseresult from resu").show
  }
}