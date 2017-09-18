package  it.nowicki.jaroslaw

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object MainExample {
  def main(arg: Array[String]) {
    var logger = Logger.getLogger(this.getClass())
    if (arg.length < 2) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: MainExample  ")
      System.exit(1)
    }
    val jobName = "MainExample"
    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)
    val pathToFiles = arg(0)
    val outputPath = arg(1)
    logger.info("=> jobName \"" + jobName + "\"")
    logger.info("=> pathToFiles \"" + pathToFiles + "\"")
    val files = sc.textFile(pathToFiles)
    // do your work here
    val rowsWithoutSpaces = files.map(_.replaceAll(" ", ","))
    // and save the result
    rowsWithoutSpaces.saveAsTextFile(outputPath)
  }
}
