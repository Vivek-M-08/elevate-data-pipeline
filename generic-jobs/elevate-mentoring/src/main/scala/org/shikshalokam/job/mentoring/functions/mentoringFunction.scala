package org.shikshalokam.job.mentoring.functions

import org.apache.spark.sql.functions.{col, expr, lit, sum}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.Properties
import scala.util.{Failure, Success, Try}

abstract class mentoringFunction {

  val configProperties: Properties = new Properties()
  configProperties.load(getClass.getResourceAsStream("/application.properties"))
  val dbUser: String = configProperties.getProperty("db.user")
  val dbPassword: String = configProperties.getProperty("db.password")
  val url: String = configProperties.getProperty("url")

  def readFromPostgres(url: String, database: String, query: String, user: String, password: String, spark: SparkSession): DataFrame = {
    spark.read.format("jdbc")
      .option("url", s"${url}/$database")
      .option("dbtable", s"($query) as subquery")
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .load()
  }

  def processScriptLevelData(inputData: ujson.Value, MappingData: ujson.Value, outputData: ujson.Obj, spark: SparkSession) = {
    // Check if the JSON object has "fetchID" key
    if (inputData.obj.contains("fetchId")) {
      val fetchIdObj = inputData("fetchId")
      val idList = fetchIdData(fetchIdObj, spark)
      println(fetchIdObj)
      println(idList.length)

      var df: DataFrame = spark.emptyDataFrame // Move df declaration outside the loop

      if (inputData.obj.contains("tasks")) {
        for (160 <- idList) {
          println("_________________________________________________")
          println(160)
          val tasksArr = inputData("tasks")
          println(tasksArr)
          // Assuming processChild returns a DataFrame
          df = processTasks(tasksArr.arr, spark, 160)
          //          df = data_mapping_transformation_process(df, data_mapping, spark, pgConnectionProperties)
          //          println("lets check the output data schema")
          //          df = output_data_scheama_checking_process(df, output_mapping, spark, pgConnectionProperties)
          //          df.show()
          //          write_data_to_postgres(df, pgConnectionProperties)
        }
      }

      df

    } else {
      println("hi")
    }
  }


  def fetchIdData(fetchIdObj: ujson.Value, spark: SparkSession): List[Int] = {
    try {
      val query = fetchIdObj("query").str
      val dbName = fetchIdObj("database_name").str
      val id = fetchIdObj("id").str

      //Todo remove bellow print statement
      println("\nprinting fetchID query")
      println(query)
      println(dbName)
      println(id)
      println(s"${url}/$dbName")

      val fetchId_Df: DataFrame = readFromPostgres(url, dbName, query, dbUser, dbPassword, spark)
      fetchId_Df.show() //Todo: remove
      /**
       * Extract values from the DataFrame to type List
       */
      val idList: List[Int] = fetchId_Df.select(id).rdd.map(r => r.getInt(0)).collect().toList
      idList
    } catch {
      case e: Exception =>
        println(s"Error processing Spark DataFrame: ${e.getMessage}") //Todo: remove/ throw exception/ Don't pass empty list
        List.empty[Int]
    }
  }


  def processTasks(tasksArr: ujson.Arr, spark: SparkSession, value: Int): DataFrame = {
    val taskLen: Int = tasksArr.arr.length
    var joinedDF: DataFrame = null
    println("pringing taskLen = " + taskLen)
    println("pringing tasksArr = " + tasksArr)


    for (j <- 0 until taskLen) {
      if (j == 0) {
        println(tasksArr(0))
        if (tasksArr(0).obj.contains("single_process")) {
          val df = singleProcess(tasksArr(0)("single_process"), spark, value)
          joinedDF = df
        }
      } else {
        println(tasksArr(j))
        if (tasksArr(j).obj.contains("single_process")) {
          var df2 = singleProcess(tasksArr(j)("single_process"), spark, value)
          println("8********************")
          df2.show(false)
          val join_on = tasksArr(j)("single_process")("join_on").str
          val join_type = tasksArr(j)("single_process")("join_type").str
          val agg_type = tasksArr(j)("single_process")("agg").str
          val agg_on = tasksArr(j)("single_process")("agg_on").str
          val rename = tasksArr(j)("single_process")("rename").str
          val groupby = tasksArr(j)("single_process")("groupby").str

          println("before_df2")
          df2.show()

          if (agg_type != "none" & agg_on != "none" & groupby != "none") {
            df2 = df2.groupBy(col(s"$groupby")).agg(expr(s"$agg_type($agg_on)").alias(rename))
            println("after_df2")
            df2.show()
          }

          joinedDF = df2.join(joinedDF, Seq(join_on), join_type)
          joinedDF.show()

        } else if (tasksArr(j).obj.contains("fetchID_with_InputID")) {
          val idList2: List[Int] = processFetchID_with_InputID(tasksArr(j)("fetchID_with_InputID"), spark, value)
          val join_on2 = tasksArr(j)("fetchID_with_InputID")("join_on").str
          val join_type2 = tasksArr(j)("fetchID_with_InputID")("join_type").str
          println("@@@@@@@@@@@@@@@@@@@@@@")
          println(idList2)
          println(tasksArr(j).obj.contains("child"))

          if (tasksArr(j).obj.contains("child")) {
            val child_data2 = tasksArr(j)("child").arr
            println(child_data2)
            var df3 = processChildWithMultipleProcess(child_data2, spark, idList2, value)
            joinedDF = df3.join(joinedDF, Seq(join_on2), join_type2)
          }
        }
      }
    }
    joinedDF
  }


    def singleProcess(singleProcessData: ujson.Value, spark: SparkSession, value: Int): DataFrame = {
      try {
        //TODO: can remove input key in code and json
        val input = singleProcessData("input").str
        val query = singleProcessData("query").str
        val databaseName = singleProcessData("database_name").str
        val agg = singleProcessData("agg").str
        val agg_on = singleProcessData("agg_on").str
        val substitutedQuery = query.replace("${id}", value.toString)

        // Spark process to read data from JDBC source
        if (agg != "none" & agg_on != "none") {
          println("++++++++++++++++++++++++++++++++\n")
          var singleProcessDf = readFromPostgres(url, databaseName, substitutedQuery, dbUser, dbPassword, spark)
          singleProcessDf = singleProcessDf.na.fill(0).withColumn(agg_on, col(agg_on).cast("integer")) //TODO: bring this type case to the json instead of hard coding
          singleProcessDf.show(false)
          singleProcessDf = singleProcessDf.withColumn(input, lit(value))
          singleProcessDf.show(false)
          singleProcessDf
        }
        else {
          println("++++++++++++++++++++++++++++++++\n")
          var singleProcessDf = readFromPostgres(url, databaseName, substitutedQuery, dbUser, dbPassword, spark)
          singleProcessDf = singleProcessDf.na.fill(0)
          singleProcessDf.show(false)
          singleProcessDf = singleProcessDf.withColumn(input, lit(value))
          singleProcessDf.show(false)
          singleProcessDf
        }
        //TODO: Throw an exception saying query failed with no data
        //      if (singleProcessDf.count == 0) {
        //        // Create a new DataFrame with a single column "response" and value 0
        //        val newSchema = StructType(Array(StructField(agg_on, IntegerType, true: Boolean)))
        //        val newData = Seq(Row(0))
        //        val newRdd = spark.sparkContext.parallelize(newData)
        //        singleProcessDf = spark.createDataFrame(newRdd, newSchema)
        //      }

        //      singleProcessDf = singleProcessDf.withColumn(input, lit(value))
        //      singleProcessDf.show()
        //      singleProcessDf

      } catch {
        case e: Exception =>
          println(s"Error processing Spark DataFrame: ${e.getMessage}")
          spark.emptyDataFrame
      }
    }


  def processFetchID_with_InputID(FetchID_with_InputID: ujson.Value, spark: SparkSession, value: Int): List[Int] = {
    try {
      val query = FetchID_with_InputID("query").str
      val databaseName = FetchID_with_InputID("database_name").str
      val select_id = FetchID_with_InputID("id").str
      val substitutedQuery = query.replace("${id}", value.toString)
      var processFetchIdDf = readFromPostgres(url, databaseName, substitutedQuery, dbUser, dbPassword, spark)
      println("after")
      processFetchIdDf.show(false)

      // Extract values from the DataFrame
      val idList: List[Int] = processFetchIdDf.select(select_id).rdd.map(r => r.getInt(0)).collect().toList
      idList
    } catch {
      case e: Exception =>
        println(s"Error processing Spark DataFrame: ${e.getMessage}")
        List.empty[Int]
    }
  }

  def processChildWithMultipleProcess(child: ujson.Value, spark: SparkSession, idList2: List[Int], value: Int): DataFrame = {
    val childLen = child.arr.length
    println(s"childLen:$childLen")
    var joinedDF: DataFrame = null

    for (j <- 0 until childLen) {
      if (j == 0) {
        if (child(j).obj.contains("multiple_process")) {
          var df = processMultipleProcess(child(j)("multiple_process"), spark, idList2, value)
          joinedDF = df
        }
      } else if (child(j).obj.contains("multiple_process")) {
        val df = processMultipleProcess(child(j)("multiple_process"), spark, idList2, value)
        val join_on = child(j)("multiple_process")("join_on").str
        val join_type = child(j)("multiple_process")("join_type").str
        joinedDF = df.join(joinedDF, Seq(join_on), join_type)
      }
    }

    joinedDF
  }


  def processMultipleProcess(multipleProcessData: ujson.Value, spark: SparkSession, idList: List[Int], value: Int): DataFrame = {
    // Extract configuration values
    val input = multipleProcessData("input").str
    val query = multipleProcessData("query").str
    val databaseName = multipleProcessData("database_name").str
    val groupBy = multipleProcessData("groupBy").str
    val agg = multipleProcessData("agg").str
    val agg_on = multipleProcessData("agg_on").str
    val join_on = multipleProcessData("join_on").str
    val url = s"jdbc:postgresql://localhost:5432/$databaseName"
    val rename = multipleProcessData("rename").str

    // Print input details
    println(s"Multiple Process: Input: $input, Query: $query, Database: $databaseName, GroupBy: $groupBy, Aggregate: $agg, Aggregate_on: $agg_on")

    // Wrap the main logic in a Try block for error handling
    val result: DataFrame = Try {
      // Collect DataFrames in a list
      val dfs: List[DataFrame] = idList.flatMap { ids =>
        // Replace placeholders in the query
        val substitutedQuery = query.replace("${id}", ids.toString)
        println(s"substitutedQuery: $substitutedQuery")

        // Read data from PostgreSQL, replace null values with 0, and cast the 'response' column to IntegerType
        //          val df: DataFrame = spark
        //            .read.jdbc(url, s"($substitutedQuery)", pgConnectionProperties)
        //            .na.fill(0)
        //            .withColumn(agg_on, col(agg_on).cast("integer"))

        val df: DataFrame = spark.read.format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/" + databaseName) // Replace with your PostgreSQL connection URL
          .option("dbtable", s"($substitutedQuery) as subquery")
          .option("user", dbUser) // Replace with your PostgreSQL username
          .option("password", dbPassword) // Replace with your PostgreSQL password
          .option("driver", "org.postgresql.Driver")
          .load().na.fill(0)
          .withColumn(agg_on, col(agg_on).cast("integer"))
        df.show(false)


        if (df.count == 0) {
          // Create a new DataFrame with a single column "response" and value 0
          val newSchema = StructType(Array(StructField(agg_on, IntegerType, true: Boolean)))
          val newData = Seq(Row(0))
          val newRdd = spark.sparkContext.parallelize(newData)
          val newDf = spark.createDataFrame(newRdd, newSchema)
          List(newDf)
        } else {
          println(s"printing df :")
          df.show()
          List(df) // Wrap the DataFrame in a List to make it TraversableOnce
        }
      }

      // Union all DataFrames, including those with zero values
      var resultDF: DataFrame = dfs.reduceOption(_ unionAll _).getOrElse(spark.emptyDataFrame)

      // Perform grouping and aggregation, and add a new column 'join_on' with a constant value
      resultDF = resultDF.withColumn(join_on, lit(value))
      val groupedResultDF = resultDF.groupBy(col(join_on)).agg(sum(agg_on).alias(rename))
      groupedResultDF.show()
      groupedResultDF // Return the grouped DataFrame
    } match {
      case Success(result) => result
      case Failure(exception) =>
        println(s"Error processing Spark DataFrame: ${exception.getMessage}")
        spark.emptyDataFrame // Return an empty DataFrame or handle the exception as needed
    }
    result
  }


  }
