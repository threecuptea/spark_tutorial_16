package org.freemind.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


/**
  * Created by fandev on 9/15/16.
  *  Codes use Spark 1.6 can only be compil
  * originate from https://databricks.com/blog/2016/08/15/how-to-use-sparksession-in-apache-spark-2-0.html
  */
object SparkZipsExample16 {

  def main(args: Array[String]): Unit = {
    if (args.length != 1)    {
      println("Usage: SparkSessionSimpleExample16 <path_to_json_file")
      System.exit(-1)
    }

    val jsonFile:String = args(0)

    val conf = new SparkConf().setAppName("SparkZipsExample16")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    //It works on 1.6 Spark server but not Spark 2.0 server
    val lst = List(("Scala", 35), ("Python", 30), ("R", 15), ("Java", 20))
    val lpDf = sqlContext.createDataFrame(lst).toDF("language", "percent")
    lpDf.orderBy(desc("percent")).show()

    //If I use as[Zips].  it will return DataSet.  I won't be able to use DataFrame in this case. Dataset has very limited APIs in Spark 1.6.
    //In Spark 2,  Dataset consolidates methods from both DataFrame as well as RDD. DataFrame is Dataset[Row].
    //It's much convenient
    val zipDF = sqlContext.read.json(jsonFile).withColumnRenamed("_id", "zip")
    zipDF.printSchema()

    println("Any zip with Population > 40000")
    //$"xxx" convert it into column and will validate them
    zipDF.select($"state", $"city", $"zip", $"pop").filter($"pop" > 40000).orderBy(desc("pop")).show(10, false)

    println("States in order of population")
    zipDF.select("state", "pop").groupBy("state").sum("pop").orderBy(desc("sum(pop)")).show(10, false)

    println("California cities in order of population")
    //Need to use agg because I have to use both count and sum
    zipDF.filter("state = 'CA'").groupBy("city").agg(count("zip"), sum("pop"))
      .withColumnRenamed("count(zip)", "num zips").withColumnRenamed("sum(pop)", "population")
      .orderBy(desc("population")).show(10, false)


    zipDF.registerTempTable("zips_table")
    //If I don't use as, the column name will be _c1, _c2.  In spark 2.0.  It is count(zip) and sum(pop)
    //I cannot make column name "num zips" work.  It will break the space.  how do we treat a string as a whole
    println("Cities order by population with stats of num of zips")
    sqlContext.sql(s"SELECT city, count(zip) AS num_zips, sum(pop) AS population FROM zips_table GROUP BY city ORDER BY sum(pop) DESC").show(10, false)



  }


}
