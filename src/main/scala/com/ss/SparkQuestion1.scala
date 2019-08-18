package com.ss
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
//problem statement : Get all the employees  whose year of joining is between 2017 and 2018 and salary is >50.
object SparkQuestion1 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val sparkSession=SparkSession.builder().master("local[*]").appName("Interview Questions").getOrCreate()
    import sparkSession.implicits._
    val customerdf=Seq((1,"X","2019-01-01 12:02:00.0"),(2,"y","2018-01-01 12:02:00.0"),(3,"Z","2017-01-01 12:02:00.0"),(4,"F","2017-01-01 12:02:00.0"),(5,"G","2017-01-01 10:02:00.0")).toDF("cust_id","cust_name","yoj")
    val deptdf=Seq((1,100),(1,200),(2,300),(3,50),(4,50)).toDF("id","amount")
    val cust= customerdf.withColumn("datetime", date_format(col("yoj").cast(TimestampType), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
    val filteredcust=cust.filter(year($"yoj").between(2017,2019))//.show
    val filtereddept=deptdf.filter($"amount">50)
    val result=filteredcust.join(filtereddept,col("cust_id")===col("id"),"inner").show(truncate = false)
  }
}
