package com.ss
import scala.io.Source
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.concurrent.Await
import scala.concurrent.duration._
object FuturesDemo {
  def main(args: Array[String]): Unit = {
    val path1 = "C:\\Users\\Shikhar\\Desktop\\input.txt,C:\\Users\\Shikhar\\Desktop\\input1.txt,C:\\Users\\Shikhar\\Desktop\\input2.txt"
    //val path2 = "C:\\Users\\Shikhar\\Desktop\\input1.txt"
    //val path3 = "C:\\Users\\Shikhar\\Desktop\\input2.txt"

    val lis = path1.split(",")
    lis.foreach(source => donutStock(source).onComplete {
      case Success(stock) => println(s"Stock for vanilla donut = $stock")
      case Failure(e) => println(s"Failed to find vanilla donut stock, exception = $e")
    })
    Thread.sleep(9000)
    println("I am at the exit")

    def donutStock(donut: String): Future[Unit] = Future {
      // assume some long running database operation
      for(line<-Source.fromFile(donut).getLines()){println(line)
      Thread.sleep(300)}

    }

  }


}
