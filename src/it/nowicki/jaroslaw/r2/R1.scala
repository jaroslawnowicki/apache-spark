package it.nowicki.jaroslaw

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object R1 {

  def main(args: Array[String]): Unit = {

    if (arg.length < 2) {
      System.exit(1)
    }

    var t: Array[String] = ["dziala"]



    val jobName = "R1"
    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    val pathToFiles = arg(0)
    val files = sc.textFile(pathToFiles)
    println(files.first)
    val head = files.take(10)
    println(head)
    println(head.length)
    head.foreach(println)


  }

  def isHeader(line: String) : Boolean = {
    line.contains("id_1")
  }
}