package com.ss

import org.apache.spark.sql.SparkSession

object SparkQuestion4 {
  def main(args: Array[String]): Unit = {
    //Find avg salary department wise using rdd approach
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val sparkSession = SparkSession.builder().master("local[*]").appName("Interview Questions").getOrCreate()
 sparkSession.sparkContext.setLogLevel("error")
    val inputRdd = sparkSession.sparkContext.parallelize(Seq("1,'RAM',10","2,'Shyam',30","1,'Vijay',10"))
    inputRdd.map(record=>record.split(","))
      .map(record=>(record(0).toInt,(record(2).toInt,1)))
      .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .mapValues(x=>x._1/x._2)
      .collect.foreach(println)
  }
}
