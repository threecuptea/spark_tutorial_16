package org.freemind.spark.sql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable.WrappedArray

/**
  * http://cdn2.hubspot.net/hubfs/438089/notebooks/MongoDB_guest_blog/Using_MongoDB_Connector_for_Spark.html
  *
  * Using convert_csv.py to convert "::" delimiter to "," then
  * mongoimport -d movielens -c movie_ratings --type csv -f user_id,movie_id,rating,timestamp data/ratings.csv
  *
  * movieDS has the following issue
  * com.mongodb.spark.exceptions.MongoTypeConversionException: Cannot cast STRING into a ConflictType (value: BsonString{value='Toy Story (1995)'})
  * https://groups.google.com/forum/#!msg/mongodb-user/lQjppYa21mQ/XrPfJAccBgAJ
  *
  *
  * $SPARK_HOME/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.10:1.1.0 \
  * --master local[*] --class org.freemind.spark.sql.MovieLensALSMongo16 target/scala-2.10/spark_tutorial_16_2.10-1.0.jar
  *
  * @author sling(threecuptea) wrote on 1/29/17.
  */


case class MongoRating(user_id: Int, movie_id: Int, rating: Float)
case class MongoMovieId(movie_id: Int)
case class MongoMovie(id: Int, title: String, genres: Array[String])

object MovieLensALSMongo16 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MovieLensALSMongo16")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //In Spark 1.6, the following line is needed to understand $"id", $"rating" those syntax,
    import sqlContext.implicits._

    val mrReadConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/movielens.movie_ratings?readPreference=primaryPreferred"))
    val prReadConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/movielens.personal_ratings?readPreference=primaryPreferred"))
    val movieReadConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/movielens.movies?readPreference=primaryPreferred"))
    val recomWriteConfig = WriteConfig(Map("uri" -> "mongodb://localhost:27017/movielens.recommendations"))

    //MongoSpark.load(sqlContext, mrReadConfig) return MongoRDD.  That's why map works here
    val mrDS = MongoSpark.load(sqlContext, mrReadConfig).map(r => MongoRating(r.getAs[Int]("user_id"), r.getAs[Int]("movie_id"), r.getAs[Int]("rating"))).toDF().cache()
    val prDS = MongoSpark.load(sqlContext, prReadConfig).map(r => MongoRating(r.getAs[Int]("user_id"), r.getAs[Int]("movie_id"), r.getAs[Int]("rating"))).toDF().cache()
    val movieDS = MongoSpark.load(sqlContext, movieReadConfig).map(r => MongoMovie(r.getAs[Int]("id"), r.getAs[String]("title"), r.getAs[String]("genre_concat").split("\\|"))).toDF().cache()
    val bMovieDS = sc.broadcast(movieDS)

    println(s"Rating Snapshot= ${mrDS.count}, ${prDS.count}")
    mrDS.show(10, false)
    //println(s"Movies Snapshot= ${bMovieDS.value.count}")
    //bMovieDS.value.show(10, false)


    val Array(trainDS, valDS, testDS) = mrDS.randomSplit(Array(0.8, 0.1, 0.1)) //Follow the instruction of EDX class, use the model getting from validationSet on test
    println(s"training count=${trainDS.count}")
    println(s"validation count=${valDS.count}")
    println(s"test count=${testDS.count}")
    val total = trainDS.count() + valDS.count() + testDS.count()
    println(s"TOTAL COUNT=$total")
    println()

    valDS.cache()
    testDS.cache()

    val trainPlusDS = trainDS.unionAll(prDS).cache() //Spark 1.6 use unionAll, Spark 2 use unit
    val allDS = mrDS.unionAll(prDS).cache()

    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
    val avgRating = trainDS.select(mean($"rating")).first().getDouble(0)

    val baselineRmse = evaluator.evaluate(testDS.withColumn("prediction", lit(avgRating)))
    printf("The baseline rmse= %3.2f.\n", baselineRmse)
    println()

    /**
      * ALS codes starts here
      * ALS (alternating least square is a popular model that spark-ml use for 'Collaborative filtering'
      */
    //I tried more than those to narrow down the followings,
    val start = System.currentTimeMillis()
    val ranks = Array(10, 12) //numbers of latent factor used to predict missing entries of user-item matrics, the default is 10
    val iters = Array(20) //the default is 10
    //It is a lambda multipler on the cost to prevent overfitting.  Increasing lambda will penalize the cost which are coefficients of linear regression
    //The default is 1.0
    val regParams = Array(0.1, 0.01)

    val als = new ALS().setUserCol("user_id").setItemCol("movie_id").setRatingCol("rating")

    val paramGrids = new ParamGridBuilder()
      .addGrid(als.rank, ranks)
      .addGrid(als.maxIter, iters)
      .addGrid(als.regParam, regParams)
      .build() //build return Array[ParamMap]

    var bestRmse = Double.MaxValue
    var bestModel: Option[ALSModel] = None
    var bestParam: Option[ParamMap] = None

    for (paramMap <- paramGrids) {
      val model = als.fit(trainPlusDS, paramMap)
      val prediction = model.transform(valDS).filter(!$"prediction".isNaN)
      val rmse = evaluator.evaluate(prediction)
      //NaN is bigger than maximum
      if (rmse < bestRmse) {
        bestRmse = rmse
        bestModel = Some(model)
        bestParam = Some(paramMap)
      }
    }


    bestModel match {
      case None =>
        println("Unable to find a good ALSModel.  STOP here")
        System.exit(-1)
      case Some(goodModel) =>
        //We still need to filter out NaN
        val testPrediction = goodModel.transform(testDS).filter(!$"prediction".isNaN)
        val testRmse = evaluator.evaluate(testPrediction)
        val improvement = (baselineRmse - testRmse) / baselineRmse * 100
        println(s"The best model was trained with param = ${bestParam.get}")
        println()
        printf("The RMSE of the bestModel on test set is %3.2f, which is %3.2f%% over baseline.\n", testRmse, improvement) //use %% to print %
    }

    //Recall bestModel was fit with trainPlusDS, which cover userId=0 but might miss some movie
    //Need to study machine learning course
    val augmentModel = als.fit(allDS, bestParam.get)
    //In Spark2, I can use .map(_.movie_id) because DataSet is typed object.
    //In Spark 1.6, DataSet has limited method, we have to stay with DataFrame, Use select column instead
    val pMovieIds = prDS.select($"movie_id").collect().map(r => r.getAs[Int](0))
    val pUserId = 0

    val unratedRdd = allDS.select($"movie_id").distinct().rdd.filter(r => !pMovieIds.contains(r.getAs[Int](0)))
    val unratedDF =  unratedRdd.map({
      case Row(val1: Int) => MongoMovieId(val1)
    }).toDF().withColumn("user_id", lit(pUserId))

    //com.mongodb.spark.exceptions.MongoTypeConversionException: Cannot cast 4.591038 into a BsonValue. FloatType has no matching BsonValue.  Try DoubleType
    val recommendation = augmentModel.transform(unratedDF).sort(desc("prediction")).cache()
    //val recommendation = recommend.join(bMovieDS.value, recommend("movie_id") === bMovieDS.value("id"), "inner")

    recommendation.show(50, false)

    printf("Execution time= %7.3f seconds\n", (System.currentTimeMillis() - start)/1000.00)
    //Mongodb does not have Float type
    MongoSpark.save(recommendation.select($"movie_id", $"prediction".cast(DoubleType)).write.mode("overwrite"), recomWriteConfig)

    printf("Execution time= %7.3f seconds, including write to Mongo.\n", (System.currentTimeMillis() - start)/1000.00)

  }
}
