package de.hpi.spark_tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    print("test halloooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo")
    // TODO
  }

  def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Loading data
    //------------------------------------------------------------------------------------------------------------------

    // Create a Dataset programmatically
    val numbers = spark.createDataset((0 until 100).toList)

    // Read a Datasets from a file
    val regions = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv("../../TPCH/TPCH/tpch_region.csv")
      .as[(Int, String, String)]

    val nations = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv("../../TPCH/TPCH/tpch_nation.csv")
      .as[(Int, String, Int, String)]

    println("---------------------------------------------------------------------------------------------------------")

    regions.printSchema()
    nations.printSchema()
  }
}
