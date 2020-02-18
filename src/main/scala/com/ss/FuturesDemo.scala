package com.ss
import scala.io.Source
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.concurrent.Await
import scala.concurrent.duration._
object FuturesDemo {
  def main(args: Array[String]): Unit = {
    def donutStock(donut: String): Future[Int] = Future {
      // assume some long running database operation
      for(line<-Source.fromFile(donut).getLines()){println(line)
        Thread.sleep(200)}
      1

    }
    val path1 = "C:\\Users\\Shikhar\\Desktop\\input.txt,C:\\Users\\Shikhar\\Desktop\\input1.txt,C:\\Users\\Shikhar\\Desktop\\input2.txt,C:\\Users\\Shikhar\\Desktop\\input6.txt"
    val path2 = "C:\\Users\\Shikhar\\Desktop\\input4.txt,C:\\Users\\Shikhar\\Desktop\\input5.txt"
    //val path3 = "C:\\Users\\Shikhar\\Desktop\\input2.txt"
    var first_list=0
    val lis = path1.split(",")

    val u=lis.foreach(source => donutStock(source).onComplete {
      case Success(y) => {first_list+=y
      println("succefully loaded " + source)}
      case Failure(e) => println(s"Failed to load, exception = $e")
    })


    Thread.sleep(10000)//Time Taken to load path1 tables

    val lis2 = path2.split(",")
    var second_list=0
    if(first_list==lis.length){
      println("DATA LOADED FOR PATH1 , NOW BEGINNING PATH2 LOAD")
      lis2.foreach(source => donutStock(source).onComplete {
        case Success(x) => {second_list+=x
        println("Data Loaded for"+source)}
        case Failure(e) => println(s"Failed to load, exception = $e")
      })
    }
    Thread.sleep(10000) //Time taken for path2 data to load

    if(second_list==lis2.length){
      print("Data loaded for PATH2")
    }




  }


}