package it.nowicki.jaroslaw

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger

object RecommenderSystem {

  var logger = Logger.getLogger(this.getClass())

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.exit(1)
    }

    val jobName = "RecommenderSystem"
    val sc = new SparkContext(new SparkConf().setAppName(jobName))
    val base = args(0)
    val rawUserArtistData = sc.textFile(base + "user_artist_data.txt")
    val rawArtistData = sc.textFile(base + "artist_data.txt")
    val rawArtistAlias = sc.textFile(base + "artist_alias.txt")

    execute(args(1), rawArtistData, rawUserArtistData, rawArtistAlias, sc)
  }

  def execute(arg: String, rawArtistData: RDD[String], rawUserArtistData: RDD[String], rawArtistAlias: RDD[String], sc: SparkContext ): Unit = arg match {
      case "1" => preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
      case "2" => model(sc, rawUserArtistData, rawArtistData, rawArtistAlias)
      case "3" => evaluate(sc, rawUserArtistData, rawArtistAlias)
      case "4" =>   recommend(sc, rawUserArtistData, rawArtistData, rawArtistAlias)
      case _ => println("bad parametr")
  }

  def buildArtistByID(rawArtistData: RDD[String]) =
    rawArtistData.flatMap({ line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    })

  def buildArtistAlias(rawArtistAlias: RDD[String]): Map[Int, Int] =
    rawArtistAlias.flatMap({ line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }).collectAsMap()

  def preparation(rawUserArtistData: RDD[String], rawArtistData: RDD[String], rawArtistAlias: RDD[String]) = {
    val userIDStats = rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
    val itemIDStats = rawUserArtistData.map(_.split(' ')(1).toDouble).stats()

    logger.info("userIDStats " + userIDStats)
    logger.info("itemIDStats " + itemIDStats)

    val artistByID = buildArtistByID(rawArtistData)
    val artistAlias = buildArtistAlias(rawArtistAlias)

    val (badID, goodID) = artistAlias.head
    logger.info(artistByID.lookup(badID) + " -> " + artistByID.lookup(goodID))
  }

  def buildRatings(rawUserArtistData: RDD[String], bArtistAlias: Broadcast[Map[Int, Int]]) = {
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }
  }

  def model(sc: SparkContext, rawUserArtistData: RDD[String], rawArtistData: RDD[String], rawArtistAlias: RDD[String]): Unit = {

    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))

    val trainData = buildRatings(rawUserArtistData, bArtistAlias).cache()

    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

    trainData.unpersist()

    logger.info(model.userFeatures.mapValues(_.mkString(", ")).first())

    val userID = 2093760
    val recommendations = model.recommendProducts(userID, 5)
    recommendations.foreach(println)
    val recommendedProductIDs = recommendations.map(_.product).toSet

    val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).
      filter { case Array(user, _, _) => user.toInt == userID }

    val existingProducts = rawArtistsForUser.map { case Array(_, artist, _) => artist.toInt }.
      collect().toSet

    val artistByID = buildArtistByID(rawArtistData)

    artistByID.filter { case (id, name) => existingProducts.contains(id) }.
      values.collect().foreach(println)
    artistByID.filter { case (id, name) => recommendedProductIDs.contains(id) }.
      values.collect().foreach(println)

    unpersist(model)
  }

  def areaUnderCurve(positiveData: RDD[Rating], bAllItemIDs: Broadcast[Array[Int]], predictFunction: (RDD[(Int, Int)] => RDD[Rating])) = {

    val positiveUserProducts = positiveData.map(r => (r.user, r.product))
    val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)

    val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {

      userIDAndPosItemIDs => {

        val random = new Random()
        val allItemIDs = bAllItemIDs.value
        userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
          val posItemIDSet = posItemIDs.toSet
          val negative = new ArrayBuffer[Int]()
          var i = 0

          while (i < allItemIDs.size && negative.size < posItemIDSet.size) {
            val itemID = allItemIDs(random.nextInt(allItemIDs.size))
            if (!posItemIDSet.contains(itemID)) {
              negative += itemID
            }
            i += 1
          }

          negative.map(itemID => (userID, itemID))
        }
      }
    }.flatMap(t => t)

    val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)

    positivePredictions.join(negativePredictions).values.map {
      case (positiveRatings, negativeRatings) =>

        var correct = 0L
        var total = 0L

        for (positive <- positiveRatings;
             negative <- negativeRatings) {

          if (positive.rating > negative.rating) {
            correct += 1
          }
          total += 1
        }

        correct.toDouble / total
    }.mean()
  }

  def predictMostListened(sc: SparkContext, train: RDD[Rating])(allData: RDD[(Int, Int)]) = {
    val bListenCount =
      sc.broadcast(train.map(r => (r.product, r.rating)).reduceByKey(_ + _).collectAsMap())
    allData.map { case (user, product) =>
      Rating(user, product, bListenCount.value.getOrElse(product, 0.0))
    }
  }

  def evaluate(
                sc: SparkContext,
                rawUserArtistData: RDD[String],
                rawArtistAlias: RDD[String]): Unit = {
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))

    val allData = buildRatings(rawUserArtistData, bArtistAlias)
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()

    val allItemIDs = allData.map(_.product).distinct().collect()
    val bAllItemIDs = sc.broadcast(allItemIDs)

    val mostListenedAUC = areaUnderCurve(cvData, bAllItemIDs, predictMostListened(sc, trainData))


    val evaluations =
      for (rank <- Array(10, 50);
           lambda <- Array(1.0, 0.0001);
           alpha <- Array(1.0, 40.0))
        yield {
          val model = ALS.trainImplicit(trainData, rank, 10, lambda, alpha)
          val auc = areaUnderCurve(cvData, bAllItemIDs, model.predict)
          unpersist(model)
          ((rank, lambda, alpha), auc)
        }

    evaluations.sortBy(_._2).reverse.foreach(println)

    trainData.unpersist()
    cvData.unpersist()
  }

  def recommend(
                 sc: SparkContext,
                 rawUserArtistData: RDD[String],
                 rawArtistData: RDD[String],
                 rawArtistAlias: RDD[String]): Unit = {

    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))
    val allData = buildRatings(rawUserArtistData, bArtistAlias).cache()
    val model = ALS.trainImplicit(allData, 50, 10, 1.0, 40.0)
    allData.unpersist()

    val userID = 2093760
    val recommendations = model.recommendProducts(userID, 5)
    val recommendedProductIDs = recommendations.map(_.product).toSet

    val artistByID = buildArtistByID(rawArtistData)

    artistByID.filter { case (id, name) => recommendedProductIDs.contains(id) }.
      values.collect().foreach(println)

    val someUsers = allData.map(_.user).distinct().take(100)
    val someRecommendations = someUsers.map(userID => model.recommendProducts(userID, 5))
    someRecommendations.map(
      recs => recs.head.user + " -> " + recs.map(_.product).mkString(", ")
    ).foreach(println)

    unpersist(model)
  }

  def unpersist(model: MatrixFactorizationModel): Unit = {
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }

}