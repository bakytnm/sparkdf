import ch.hsr.geohash.GeoHash
import com.opencagedata.geocoder.OpenCageClient
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.Seq

object Main {
  def main(args: Array[String]): Unit = {
    val fFile: String = args(0)
    val zipFiles: String = args(1)
    val fKey: String = args(2)
    val unzippedFolder = zipFiles + "unzipped"
    val sUnzip: String = args(3)
    val fJoined: String = args(4)
    val bUnzip: Boolean = sUnzip.toBoolean

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DFHomework")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // load DataFrame from file
    var restaurants = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load(fFile)

    // OpenCageClient is used to retrieve geo positions based on address location
    val geoClient = new OpenCageClient(fKey)

    println("Records with empty lat or lng:")
    restaurants.filter("lat is null or lng is null").show(false)

    // Records with no lat/lng values grouped into new RDD, then converted into DataFrame.
    // lat/lng values taken from OpenCage Geocoding API
    val badRDD:Array[Row] =  restaurants.filter(col("lat").isNull || col("lng").isNull).collect.map(row => {
      val GeoPos = GeoInfo.getGeoPos(row.getAs[String]("country") + ", " + row.getAs[String]("city"), geoClient)
      Row(row.getAs[String]("id"),
        row.getAs[String]("franchise_id"),
        row.getAs[String]("franchise_name"),
        row.getAs[String]("restaurant_franchise_id"),
        row.getAs[String]("country"),
        row.getAs[String]("city"),
        GeoPos.lat.toString, GeoPos.lng.toString)
    })
    val badRowRDD = spark.sparkContext.parallelize(badRDD)
    val brokenData = spark.createDataFrame(badRowRDD, restaurants.schema)
    brokenData.show(false)

    println("Initial total records in DF: " + restaurants.count())
    // Broken records replaced by the records from "brokenData"
    restaurants = restaurants.filter(col("lat").isNotNull && col("lng").isNotNull).union(brokenData)

    println("Total records after join: " + restaurants.count())
    println("Broken records after join: " + restaurants.filter(col("lat").isNull || col("lng").isNull).count())

    // New column "geo_hash" added
    val getHash: (Double, Double) => String = (lat: Double, lng: Double) => GeoHash.geoHashStringWithCharacterPrecision (lat, lng, 4)
    val getHashUDF: UserDefinedFunction = udf(getHash)
    restaurants = restaurants.withColumn("geo_hash", getHashUDF(col("lat"), col("lng")))

    // Unzip Weather files
    if (bUnzip)
      Unzipper.run(zipFiles, unzippedFolder)

    // read weather data into DataFrame
    var weather = spark.read.format("parquet").load(unzippedFolder)
    weather = weather.withColumn("geo_hash", getHashUDF(col("lat"), col("lng")))      // 112,394,743 records in weather table

    // Optimize weather table, remove extra records with the same geohash values
    val grouped = weather.groupBy("year", "month", "day", "geo_hash").agg(
      round(avg("avg_tmpr_f"), 1).as("avg_f"),
      round(avg("avg_tmpr_c"), 1).as("avg_c"),
      round(avg("lat"), 4).as("avg_lat"),
      round(avg("lng"), 4).as("avg_lng"),
    )     // 31,882,402 records in grouped table

    val joined = grouped.join(restaurants, Seq("geo_hash"), "left_outer")     // 31,991,882 records in joined
    joined.write
      .partitionBy("year", "month", "day")
      .parquet(fJoined)

    geoClient.close()
    spark.stop()
  }
}