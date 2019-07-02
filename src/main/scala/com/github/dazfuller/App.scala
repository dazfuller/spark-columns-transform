package com.github.dazfuller

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object App {

  def main(args: Array[String]): Unit = {
    val sourceFile = getClass.getResource("/data.csv").getPath

    val spark = SparkSession.builder
      .appName("demo")
      .master("local[*]")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    import spark.implicits._

    val df = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(sourceFile)

    val keyValueCols = df.columns
      .drop(1)
      .flatMap(c => Seq(lit(c), col(c)))

    val transformed = df
      .withColumn("collated", map(keyValueCols: _*))
      .select($"StoreId", explode($"collated"))
      .select($"StoreId", split($"key", "-").as("_key"), $"value")
      .select(
        $"StoreId",
        trim($"_key".getItem(0)).as("Category"),
        trim($"_key".getItem(1)).as("Type"),
        $"value".as("ItemsSold")
      )

    val summary = transformed
      .groupBy($"Category", $"Type")
      .agg(avg($"ItemsSold").as("AvgItemsSold"))
      .orderBy($"Category", $"Type")

    transformed.show
    summary.show
  }
}
