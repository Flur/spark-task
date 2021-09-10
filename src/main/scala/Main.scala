import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{DataType, DataTypes, DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{explode, hour, lit, round, to_date, to_timestamp}

object Main {

  val sc: SparkSession = SparkSession.builder
    .master("local")
    .appName("Hotel Bids")
    .getOrCreate()

  def main(args: Array[String]) {
    import sc.implicits._

    val bids = prepareBids()
    val exchangeRateBroadcast = prepareExchangeRate()
    val motels = readMotels(sc, "src/main/resources/local_smaller/motels.txt")

    val bidsWithUsd = dealingWithBids(bids, exchangeRateBroadcast)

    bidsWithUsd
      .join(motels, $"MotelID" === $"id")
      .groupBy($"MotelID", $"MotelName", $"date", $"Country").agg(functions.max($"Losa").as("Losa"))
      .explain()
  }

  def dealingWithBids(bids: DataFrame, exchangeRateBroadcast: Broadcast[scala.collection.Map[String, Double]]): DataFrame = {
    import sc.implicits._

    implicit val enco: ExpressionEncoder[Row] = RowEncoder(new StructType()
      .add("MotelID", IntegerType, nullable = false)
      .add("BidDate", StringType, nullable = true)
      .add("US", DoubleType, nullable = true)
      .add("MX", DoubleType, nullable = true)
      .add("CA", DoubleType, nullable = true)
    )

    bids
      .na.drop("any", Seq("US", "MX", "CA"))
      .select($"MotelID", $"BidDate", $"US", $"MX", $"CA")
      .map((r: Row) => {
        val usdToEurRate: Double = exchangeRateBroadcast.value.get(r.getString(1)) match {
          case Some(v) => v
          case None => 1
        }

        Row(r(0), r(1), r.getDouble(2) * usdToEurRate, r.getDouble(3) * usdToEurRate, r.getDouble(4) * usdToEurRate)
      })
      .select($"MotelID", $"BidDate", round($"US", 3).as("US"), round($"MX", 3).as("MX"), round($"CA", 3).as("CA"))
      .select($"MotelID", to_timestamp($"BidDate", "HH-dd-MM-yyyy").as("date"),
        explode(
          functions.map(lit("US"), $"US", lit("MX"), $"MX", lit("CA"), $"CA"))
          .as(Seq("Country", "Losa"))
      )
    //      .explain()
  }

  def prepareExchangeRate(): Broadcast[scala.collection.Map[String, Double]] = {
    import sc.implicits._

    val exchangeRate = readExchangeRate(sc, "src/main/resources/local_smaller/exchange_rate.txt")

    val exchangeRateMap = exchangeRate
      .select($"ValidFrom".cast("string"), $"ExchangeRate")
      .as[(String, Double)]
      .rdd
      .collectAsMap

    // 2. Exchange rates
    val exchangeRateMapBroadcast = sc.sparkContext.broadcast(exchangeRateMap)

    exchangeRateMapBroadcast
  }

  def prepareBids(): DataFrame = {
    val bidsDF = readBids(sc, "src/main/resources/local_smaller/bids.txt")

    val clearedBidsDF = clearFromErrors(bidsDF)

    clearedBidsDF
  }

  def readBids(sc: SparkSession, filePath: String): DataFrame = {
    import sc.implicits._

    var schema = new StructType()
      .add("MotelID", IntegerType, nullable = false)
      .add("BidDate", StringType, nullable = false)

    List("HU", "UK", "NL", "US", "MX", "AU", "CA", "CN", "KR", "BE", "I", "JP", "IN", "HN", "GY", "DE")
      .foreach(h => schema = schema.add(h, DoubleType, nullable = true))

    val df: Dataset[Row] = sc.read
      .options(Map("delimiter" -> ",", "header" -> "false"))
      .schema(schema)
      .csv(filePath)

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
      // todo remove on real spark!!!!
      .repartition(1)
      .write
      .format("csv")
      .option("header", "true")
    //      .save("BidErrors.csv")

    clearedBids
  }

  def readExchangeRate(sc: SparkSession, filePath: String): DataFrame = {
    import sc.implicits._

    val schema = new StructType()
      .add("ValidFrom", StringType, nullable = true)
      .add("CurrencyName", StringType, nullable = true)
      .add("CurrencyCode", StringType, nullable = true)
      .add("ExchangeRate", DoubleType, nullable = true)

    val exchangeRates = sc
      .read
      .options(Map("delimiter" -> ",", "header" -> "false"))
      .schema(schema)
      .csv(filePath)

    exchangeRates
  }

  def readMotels(sc: SparkSession, filePath: String): DataFrame = {
    import sc.implicits._

    val schema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("MotelName", StringType, nullable = true)
      .add("CountryCode", StringType, nullable = true)
      .add("URL", StringType, nullable = true)
      .add("Comment", StringType, nullable = true)

    val motels = sc
      .read
      .options(Map("delimiter" -> ",", "header" -> "false"))
      .schema(schema)
      .csv(filePath)

    motels.show()

    motels
  }
}