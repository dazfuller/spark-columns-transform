package com.github.dazfuller

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object App {

  def main(args: Array[String]): Unit = {
    // Get the path of the example file from resources
    val sourceFile = getClass.getResource("/data.csv").getPath

    val spark = SparkSession.builder
      .appName("demo")
      .master("local[*]")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    import spark.implicits._

    // Read in the example file
    val df = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(sourceFile)

    // Create a collection of column names to key-value pairs
    val keyValueCols = df.columns
      .drop(1)
      .flatMap(c => Seq(lit(c), col(c)))

    // Transform the DataFrame
    //
    // 1. Create a new column called collated where we map the key-value columns for each row
    // 2. Select only the identifier and then explode out the "collated" values
    // 3. Split out the "key" column so that we have a column with an array of the split components of the name
    // 4. Select the identifier, then select new columns based on the split parts, along with the value
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

    // Just for fun, lets generate an aggregated view of the data which would have been a lot harder
    // in it's original format
    val summary = transformed
      .groupBy($"Category", $"Type")
      .agg(avg($"ItemsSold").as("AvgItemsSold"))
      .orderBy($"Category", $"Type")

    transformed.show
    summary.show
  }
}
