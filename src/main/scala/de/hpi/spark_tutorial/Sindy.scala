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

    val nations2 = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv("../../TPCH/TPCH/tpch_nation.csv")
      .as[(String, String, String, String)]

    println("---------------------------------------------------------------------------------------------------------")

    regions.printSchema()
    nations.printSchema()

    println("---------------------------------------------------------------------------------------------------------")

   // val nat = nations.map{ case (nkey, nname, nrkey, ncomment) => nkey}
     // .show()

    println("---------------------------------------------------------------------------------------------------------")

    //regions.join(nations, $"R_REGIONKEY" === $"N_REGIONKEY", "outer")
      //  .show()

    println("---------------------------------------------------------------------------------------------------------")



    val flat = nations2.flatMap{case (key, nname, nrkey, ncomment) => Array((key, "a"), (nname, "b"), (nrkey, "c"), (ncomment, "d"))}

  /*  flat.groupByKey(t => t._1)
      .mapGroups { case (key, iterator) =>
        val arr = List("f")
        iterator.foreach(m => arr + m._2)
        //iterator.foreach(m => println(m._2))

        println(arr.size)

        val size = iterator.size
        (key, size)
      }
      .toDF("value", "size")
      .show()

   */

    println("---------------------------------------------------------------------------------------------------------")

    val test = flat
      .toDF("val", "header")

    /*
    // SQL on DataFrames
    test.createOrReplaceTempView("test") // make this dataframe visible as a table
    val sqlResult = spark.sql("SELECT val, attr FROM test WHERE test.val = 0")
      .show()

    val filledNulls = flat
      .toDF("val", "attr")
      .select(
        col("val"),
        col("attr").as("SELECT val FROM flat WHERE flat.val = flat.val"))
      .show(50)

     */

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions._
    val headerset = test.withColumn("set", collect_set("header").over(Window.partitionBy("val")))

    headerset.select("val", "set").distinct().show()

    //test.withColumn("set", collect_set("header").over(Window.partitionBy("val"))).show(false)
  }
}
