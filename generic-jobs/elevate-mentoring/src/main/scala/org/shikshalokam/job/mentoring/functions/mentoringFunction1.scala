package org.shikshalokam.job.mentoring.functions

import org.apache.spark.sql.functions.{col, lit, sum}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.Properties
import scala.util.{Failure, Success, Try}

abstract class mentoringFunction1 {

  case class ProcessResult(dataFrame: DataFrame, joinType: String, joinOn: String)

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
    var joinedDF: DataFrame = spark.emptyDataFrame
    println("pringing taskLen = " + taskLen)
    println("pringing tasksArr = " + tasksArr)

    def processTask(task: ujson.Value): ProcessResult = {
      println("+++++++++++++++++++++++++++++++++++++++++++++++++++")
      println(task)
      if (task.obj.contains("single_process")) {
        singleProcess(task("single_process"), spark, value)
      }
      else if (task.obj.contains("fetchID_with_InputID")) {
        processAgg(task, spark, value)
      }
      else {
        ProcessResult(spark.emptyDataFrame, "none", "none")
      }
    }


    for (j <- 0 until taskLen) {
      val task = tasksArr(j)
      val df = processTask(task)
      val joinDF = df.dataFrame
      val joinType = df.joinType
      val joinOn = df.joinOn
      joinDF.show(false)




      if (joinDF != spark.emptyDataFrame && joinType == "none" && joinOn == "none") {
        println("1")
        joinedDF = joinDF
      } else if (joinDF != spark.emptyDataFrame && joinType != "none" && joinOn != "none") {
        println("2")
        println("joinType = " + joinType + " joinOn = " + joinOn)
        joinedDF = joinedDF.join(joinDF, Seq(joinOn), joinType)
      } else if (joinDF == spark.emptyDataFrame && joinType == "none" && joinOn == "none") {
        println("3")
        joinedDF = joinedDF
        joinDF.drop()
      }

      //println(joinedDF)
      joinedDF.show(false)
      println("___________________________________END JOIN DF_____________________________________\n")

    }

    joinedDF
  }


  def singleProcess(singleProcessData: ujson.Value, spark: SparkSession, value: Int): ProcessResult = {

    try {
      //TODO: can remove input key in code and json
      val input = singleProcessData("input").str
      val query = singleProcessData("query").str
      val databaseName = singleProcessData("database_name").str
      val agg = singleProcessData("agg").str
      val agg_on = singleProcessData("agg_on").str
      val join_on = singleProcessData("join_on").str
      val join_type = singleProcessData("join_type").str
      val substitutedQuery = query.replace("${id}", value.toString)


      // Spark process to read data from JDBC source
      if (agg != "none" & agg_on != "none") {
        //        println("++++++++++++++++++++++++++++++++\n")
        var singleProcessDf = readFromPostgres(url, databaseName, substitutedQuery, dbUser, dbPassword, spark)
        singleProcessDf = singleProcessDf.na.fill(0).withColumn(agg_on, col(agg_on).cast("integer")) //TODO: bring this type case to the json instead of hard coding
        //singleProcessDf.show(false)
        singleProcessDf = singleProcessDf.withColumn(input, lit(value))
        //singleProcessDf.show(false)
        ProcessResult(singleProcessDf, join_type, join_on)
      }
      else {
        //        println("++++++++++++++++++++++++++++++++\n")
        var singleProcessDf = readFromPostgres(url, databaseName, substitutedQuery, dbUser, dbPassword, spark)
        singleProcessDf = singleProcessDf.na.fill(0)
        //singleProcessDf.show(false)
        singleProcessDf = singleProcessDf.withColumn(input, lit(value))
        //singleProcessDf.show(false)
        singleProcessDf
        ProcessResult(singleProcessDf, join_type, join_on)
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
        ProcessResult(spark.emptyDataFrame, null, null)
    }
  }

  def processAgg(data: ujson.Value, spark: SparkSession, value: Int): ProcessResult = {

    try {
      println(data)
      var finalMultiDf: ProcessResult = new ProcessResult(spark.emptyDataFrame, "none", "none")
      var appendDf = spark.emptyDataFrame
      val subData = data("fetchID_with_InputID")
      val query = subData("query").str
      val databaseName = subData("database_name").str
      val select_id = subData("id").str
      val substitutedQuery = query.replace("${id}", value.toString)
      var processFetchIdDf = readFromPostgres(url, databaseName, substitutedQuery, dbUser, dbPassword, spark)
      println("after")
      processFetchIdDf.show(false)
      val idList: List[Int] = processFetchIdDf.select(select_id).rdd.map(r => r.getInt(0)).collect().toList
      println(idList)
      if (data.obj.contains("child")) {
        println(data.obj("child"))
        val tasksData = data("child").arr
        println(tasksData)
        val dataLen = tasksData.arr.length

        for (j <- 0 until dataLen) {
          println("\n\n")
          println("dataLen: " + dataLen)
          val task = tasksData(j)
          println("task:" + task)
          val resultDf = multiProcess(task, spark, idList, value)
          println("@@@@@@&&&&&&&&&########")
          resultDf.dataFrame.show(false)

          if (resultDf.dataFrame != spark.emptyDataFrame && resultDf.joinType == "none" && resultDf.joinOn == "none") {
            println("1")
            println("joinType=" + resultDf.joinType + " joinOn=" + resultDf.joinOn)
            appendDf = resultDf.dataFrame
//            finalMultiDf = ProcessResult(appendDf, resultDf.joinType, resultDf.joinOn)
//            finalMultiDf.dataFrame.show(false)
//            println(finalMultiDf.joinType)
//            println(finalMultiDf.joinOn)
          } else if (resultDf.dataFrame != spark.emptyDataFrame && resultDf.joinType != "none" && resultDf.joinOn != "none") {
            println("2")
            println("joinType=" + resultDf.joinType + " joinOn=" + resultDf.joinOn)
            appendDf = appendDf.join(resultDf.dataFrame, Seq(resultDf.joinOn), resultDf.joinType)
//            finalMultiDf = ProcessResult(appendDf, resultDf.joinType, resultDf.joinOn)
//            finalMultiDf.dataFrame.show(false)
//            println(finalMultiDf.joinType)
//            println(finalMultiDf.joinOn)
          } else if (resultDf.dataFrame == spark.emptyDataFrame && resultDf.joinType == "none" && resultDf.joinOn == "none") {
            println("3")
            println("joinType=" + resultDf.joinType + " joinOn=" + resultDf.joinOn)
            println("You might want to log a message indicating that resultDf is empty")
//            finalMultiDf = ProcessResult(appendDf, resultDf.joinType, resultDf.joinOn)
//            resultDf.dataFrame.drop()
//            finalMultiDf.dataFrame.show(false)
//            println(finalMultiDf.joinType)
//            println(finalMultiDf.joinOn)
          }
          finalMultiDf = ProcessResult(appendDf, resultDf.joinType, resultDf.joinOn)
        } //end of for

      } //end of if
      finalMultiDf.dataFrame.show(false)
      println("final join type = " + finalMultiDf.joinType)
      println("final join on = " + finalMultiDf.joinOn)
      println("returning finalDf from processAgg method")
      finalMultiDf
    } //end of try

    catch {
      case e: Exception =>
        println(s"Error processing Spark DataFrame: ${e.getMessage}")
        ProcessResult(spark.emptyDataFrame, "none", "none")
    }
  }

  //  def processAgg(data: ujson.Value, spark: SparkSession, value: Int): ProcessResult = {
  //    try {
  //      println(data)
  //      var finalMultiDf: ProcessResult = new ProcessResult(spark.emptyDataFrame, "none", "none")
  //      var appendDf = spark.emptyDataFrame
  //      val subData = data("fetchID_with_InputID")
  //      val query = subData("query").str
  //      val databaseName = subData("database_name").str
  //      val selectId = subData("id").str
  //      val substitutedQuery = query.replace("${id}", value.toString)
  //      val processFetchIdDf = readFromPostgres(url, databaseName, substitutedQuery, dbUser, dbPassword, spark)
  //      println("after")
  //      processFetchIdDf.show(false)
  //      val idList: List[Int] = processFetchIdDf.select(selectId).rdd.map(_.getInt(0)).collect().toList
  //      println(idList)
  //
  //      if (data.obj.contains("child")) {
  //        val tasksData = data("child").arr
  //        for (task <- tasksData) {
  //          val resultDf = multiProcess(task, spark, idList, value)
  //          println("@@@@@@&&&&&&&&&########")
  //          resultDf.dataFrame.show(false)
  //
  //          if (resultDf.dataFrame != spark.emptyDataFrame && resultDf.joinType == "none" && resultDf.joinOn == "none") {
  //            println("1")
  //            println(s"joinType=${resultDf.joinType} joinOn=${resultDf.joinOn}")
  //            appendDf = resultDf.dataFrame
  //            //finalMultiDf = ProcessResult(appendDf, resultDf.joinType, resultDf.joinOn)
  //          } else if (resultDf.dataFrame != spark.emptyDataFrame && resultDf.joinType != "none" && resultDf.joinOn != "none") {
  //            println("2")
  //            println(s"joinType=${resultDf.joinType} joinOn=${resultDf.joinOn}")
  //            appendDf = appendDf.join(resultDf.dataFrame, Seq(resultDf.joinOn), resultDf.joinType)
  //            //finalMultiDf = ProcessResult(appendDf, resultDf.joinType, resultDf.joinOn)
  //          } else if (resultDf.dataFrame == spark.emptyDataFrame && resultDf.joinType == "none" && resultDf.joinOn == "none") {
  //            println("3")
  //            println("You might want to log a message indicating that resultDf is empty")
  //            // No need to modify appendDf or finalMultiDf here
  //            // You might want to log a message indicating that resultDf is empty
  //          }
  //          finalMultiDf = ProcessResult(appendDf, resultDf.joinType, resultDf.joinOn)
  //        }
  //      }
  //      finalMultiDf
  //    } catch {
  //      case e: Exception =>
  //        println(s"Error processing Spark DataFrame: ${e.getMessage}")
  //        e.printStackTrace() // Log the full stack trace for debugging
  //        ProcessResult(spark.emptyDataFrame, "none", "none")
  //    }
  //  }


  def multiProcess(multipleProcessData: ujson.Value, spark: SparkSession, idList: List[Int], value: Int): ProcessResult = {
    val task = multipleProcessData("multiple_process")
    val input = task("input").str
    val query = task("query").str
    val databaseName = task("database_name").str
    val groupBy = task("groupBy").str
    val agg = task("agg").str
    val agg_on = task("agg_on").str
    val join_on = task("join_on").str
    val join_type = task("join_type").str
    val default_column = task("default_column").str

    println(s"Multiple Process: Input: $input, Query: $query, Database: $databaseName, GroupBy: $groupBy, Aggregate: $agg, Aggregate_on: $agg_on")

    // Wrap the main logic in a Try block for error handling
    val result: ProcessResult = Try {
      val dfs: List[DataFrame] = idList.flatMap { ids =>
        val substitutedQuery = query.replace("${id}", ids.toString)
        var df = readFromPostgres(url, databaseName, substitutedQuery, dbUser, dbPassword, spark)
        df = df.na.fill(0).withColumn(agg_on, col(agg_on).cast("integer")) //TODO do the type casting later via json
        //TODO: Do a if condition
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
      resultDF = resultDF.withColumn(default_column, lit(value))
      val groupedResultDF = resultDF.groupBy(col(default_column)).agg(sum(agg_on))
      ProcessResult(groupedResultDF, join_type, join_on)
      // Return the grouped DataFrame
    } match {
      case Success(result) => result
      case Failure(exception) =>
        println(s"Error processing Spark DataFrame: ${exception.getMessage}")
        ProcessResult(spark.emptyDataFrame, null, null) // Return an empty DataFrame or handle the exception as needed
    }
    result
  }


  //  def multiProcess(multipleProcessData: ujson.Value, spark: SparkSession, idList: List[Int], value: Int): ProcessResult = {
  //    val task = multipleProcessData("multiple_process")
  //    val input = task("input").str
  //    val query = task("query").str
  //    val databaseName = task("database_name").str
  //    val groupBy = task("groupBy").str
  //    val agg = task("agg").str
  //    val agg_on = task("agg_on").str
  //    val join_on = task("join_on").str
  //    val join_type = task("join_type").str
  //    val default_column = task("default_column").str
  //
  //    println(s"Multiple Process: Input: $input, Query: $query, Database: $databaseName, GroupBy: $groupBy, Aggregate: $agg, Aggregate_on: $agg_on")
  //
  //    // Wrap the main logic in a Try block for error handling
  //    val result: ProcessResult = Try {
  //      val dfs: List[DataFrame] = idList.flatMap { ids =>
  //        val substitutedQuery = query.replace("${id}", ids.toString)
  //        var df = readFromPostgres(url, databaseName, substitutedQuery, dbUser, dbPassword, spark)
  //        df = df.na.fill(0).withColumn(agg_on, col(agg_on).cast(IntegerType)) // Type casting
  //        // Filter empty DataFrames and create new DataFrame with default value
  //        if (df.isEmpty) {
  //          val newSchema = StructType(Array(StructField(agg_on, IntegerType, true)))
  //          val newData = Seq(Row(0))
  //          val newDf = spark.createDataFrame(spark.sparkContext.parallelize(newData), newSchema)
  //          List(newDf)
  //        } else {
  //          df.show()
  //          List(df)
  //        }
  //      }
  //
  //      // Union all DataFrames
  //      val resultDF = dfs.reduceOption(_ unionAll _).getOrElse(spark.emptyDataFrame)
  //        .withColumn(default_column, lit(value))
  //        .groupBy(col(default_column)).agg(sum(agg_on))
  //
  //      ProcessResult(resultDF, join_type, join_on)
  //    } match {
  //      case Success(result) => result
  //      case Failure(exception) =>
  //        println(s"Error processing Spark DataFrame: ${exception.getMessage}")
  //        ProcessResult(spark.emptyDataFrame, null, null)
  //    }
  //    result
  //  }


}
