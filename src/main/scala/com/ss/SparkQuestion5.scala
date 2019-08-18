package com.ss
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
//PROBLEM Statement : Find the lowest salary in each department
object SparkQuestion5 {
  def main(args: Array[String]): Unit = {
    // Find the lowest salary per department
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val sparkSession = SparkSession.builder().master("local[*]").appName("Interview Questions").getOrCreate()
    sparkSession.sparkContext.setLogLevel("error")

    import sparkSession.implicits._

    val df=Seq((1,100),(1,200),(2,300),(2,50),(3,50)).toDF("id","amount")
    //Dataframes way
    val window=Window.partitionBy($"id").orderBy($"amount".asc,$"id".asc)
    val df1=df.withColumn("row_number",row_number().over(window))
    df1.select("id","amount").where("row_number=1").show()
  //Sparl SQL WAY
    df.createOrReplaceTempView("test")
    sparkSession.sql("select id, amount from (select id ,amount , row_number() over(partition by id order by amount asc, id asc) as rnumber from test) where rnumber=1").show()
  }
}
