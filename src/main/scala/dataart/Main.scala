package dataart

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object Main {

  val sc: SparkSession = SparkSession.builder
    .master("local")
    .appName("Hotel Bids")
    .getOrCreate()

  def main(args: Array[String]) {
    import sc.implicits._

    val bids = prepareBids()
    val exchangeRateBroadcast = prepareExchangeRate()
    val motels = readMotels(sc)

    val bidsWithUsd = dealingWithBids(bids, exchangeRateBroadcast)

    bidsWithUsd
      .join(motels, $"MotelID" === $"id")
      .groupBy($"MotelID", $"MotelName", $"date", $"Country").agg(functions.max($"Losa").as("Losa"))
      .show()
    //              .explain()
  }

  def dealingWithBids(bids: DataFrame, exchangeRateBroadcast: DataFrame): DataFrame = {
    import sc.implicits._

    implicit val enco: ExpressionEncoder[Row] = RowEncoder(new StructType()
      .add("MotelID", IntegerType, nullable = false)
      .add("BidDate", StringType, nullable = true)
      .add("US", DoubleType, nullable = true)
      .add("MX", DoubleType, nullable = true)
      .add("CA", DoubleType, nullable = true)
    )

    val bidsUpdated = bids
      .na.drop("any", Seq("US", "MX", "CA"))
      .select($"MotelID", $"BidDate", $"US", $"MX", $"CA")


    val bidsWithUSD = bidsUpdated
      .join(
        broadcast(exchangeRateBroadcast),
        bidsUpdated("BidDate") <=> exchangeRateBroadcast("ValidFrom")
      )
      .select(
        $"MotelID",
        $"BidDate",
        round(($"US" * $"ExchangeRate"), 3).as("US"),
        round(($"MX" * $"ExchangeRate"), 3).as("MX"),
        round(($"CA" * $"ExchangeRate"), 3).as("CA")
      )
      .select($"MotelID", to_timestamp($"BidDate", "HH-dd-MM-yyyy").as("date"),
        explode(
          functions.map(lit("US"), $"US", lit("MX"), $"MX", lit("CA"), $"CA"))
          .as(Seq("Country", "Losa"))
      )

    bidsWithUSD
  }

  def prepareExchangeRate(): DataFrame = {
    import sc.implicits._

    val exchangeRate = readExchangeRate(sc)

   exchangeRate
      .select($"ValidFrom", $"ExchangeRate")
  }

  def prepareBids(): DataFrame = {
    val bidsDF = readBids(sc)

    val clearedBidsDF = clearFromErrors(bidsDF)

    clearedBidsDF
  }

  def readBids(sc: SparkSession): DataFrame = {

    var schema = new StructType()
      .add("MotelID", IntegerType, nullable = false)
      .add("BidDate", StringType, nullable = false)

    List("HU", "UK", "NL", "US", "MX", "AU", "CA", "CN", "KR", "BE", "I", "JP", "IN", "HN", "GY", "DE")
      .foreach(h => schema = schema.add(h, DoubleType, nullable = true))

    val df: Dataset[Row] = sc.read
      .format("csv")
      .options(Map("delimiter" -> ",", "header" -> "false"))
      .schema(schema)
      .csv("src/main/resources/local_smaller/bids.txt")
    //      .load("hdfs:///tmp/data/bids2.txt")

    df
  }

  // 1. Erroneous records
  def clearFromErrors(bids: DataFrame): DataFrame = {
    import sc.implicits._

    val dfWithErrors = bids.filter($"HU".startsWith("ERROR_"))
    val clearedBids = bids.except(dfWithErrors)

    // generate output
    dfWithErrors
      .withColumn("BidDate", to_timestamp($"BidDate", "HH-dd-MM-yyyy"))
      .withColumn("error", $"HU")
      .withColumn("hour", hour($"BidDate"))
      .select($"hour", $"error")
      .groupBy($"hour", $"error").count()
      .repartition(1)
      .write
      .format("csv")
      .option("header", "true")
    //      .save("BidErrors.csv")

    clearedBids
  }

  def readExchangeRate(sc: SparkSession): DataFrame = {

    val schema = new StructType()
      .add("ValidFrom", StringType, nullable = true)
      .add("CurrencyName", StringType, nullable = true)
      .add("CurrencyCode", StringType, nullable = true)
      .add("ExchangeRate", DoubleType, nullable = true)

    val exchangeRates = sc
      .read
      .format("csv")
      .options(Map("delimiter" -> ",", "header" -> "false"))
      .schema(schema)
      //      .load("hdfs:///tmp/data/exchange_rate.txt")
      .csv("src/main/resources/local_smaller/exchange_rate.txt")

    exchangeRates
  }

  def readMotels(sc: SparkSession): DataFrame = {

    val schema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("MotelName", StringType, nullable = true)
      .add("CountryCode", StringType, nullable = true)
      .add("URL", StringType, nullable = true)
      .add("Comment", StringType, nullable = true)

    val motels = sc
      .read
      .format("csv")
      .options(Map("delimiter" -> ",", "header" -> "false"))
      .schema(schema)
      //      .load("hdfs:///tmp/data/motels.txt")
      .csv("src/main/resources/local_smaller/motels.txt")

    motels
  }
}
