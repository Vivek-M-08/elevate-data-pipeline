package org.shikshalokam.job.mentoring

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, expr, lit, round, sum}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import java.util.Properties
import scala.util.{Failure, Success, Try}

class generic_script(table_name: String, script_type: String) {
  def run(): Unit = {

    println("Staring the app")

    // Read PostgreSQL config from external file
    val configProperties: Properties = new Properties()
    configProperties.load(getClass.getResourceAsStream("/application.properties"))

    //TODO: other PSQL connection
    val pgConnectionProperties = new java.util.Properties()
    pgConnectionProperties.put("user", configProperties.getProperty("db.user"))
    pgConnectionProperties.put("password", configProperties.getProperty("db.password"))

    //TODO: INPUT config table connection
    val dbUser: String = configProperties.getProperty("db.user")
    val dbPassword: String = configProperties.getProperty("db.password")
    val inputDatabase: String = configProperties.getProperty("input.database")


    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("elevate_data_pipeline")
      .config("spark.master", "local") // Replace with your cluster configuration if not running locally
      .getOrCreate()

    // Construct the PostgreSQL query
    val query = s"SELECT * FROM $table_name WHERE script_type = '$script_type'"
    println(query)

    // Read data from PostgreSQL into a DataFrame
    val config_df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/elevate_sink_data") // Replace with your PostgreSQL connection URL
      .option("dbtable", s"($query) as subquery")
      .option("user", dbUser) // Replace with your PostgreSQL username
      .option("password", dbPassword) // Replace with your PostgreSQL password
      .option("driver", "org.postgresql.Driver")
      .load()

    // Show the DataFrame
    config_df.show(false)
    val input_data_schema = config_df.select("input_data_schema").as(Encoders.STRING).first()
    val output_data_schema = config_df.select("output_data_schema").as(Encoders.STRING).first()
    val data_mapping_schema = config_df.select("data_mapping").as(Encoders.STRING).first()
    //df.select("input_data_schema").asInstanceOf[String]
    println(input_data_schema)
    println(output_data_schema)
    println(data_mapping_schema)



    val json: ujson.Value = ujson.read(input_data_schema)
    val mappingJson:ujson.Value = ujson.read(data_mapping_schema)
    val outputJson:ujson.Value = ujson.read(output_data_schema)

    // Process the JSON structure
    val dataArray = json.arr
    println(dataArray)

    // data mapping transformation
    val dataMappingObj = mappingJson.obj
    println(dataMappingObj)

    val outputMappingObj = outputJson.obj
    println(outputMappingObj)

    dataArray.foreach { queryData =>
      processJson(queryData, dataMappingObj, outputMappingObj, spark: SparkSession, pgConnectionProperties: Properties)
    }


    def processJson(json: ujson.Value, data_mapping: ujson.Value, output_mapping: ujson.Obj, spark: SparkSession, pgConnectionProperties: Properties): DataFrame = {
      // Check if the JSON object has "fetchID" key
      if (json.obj.contains("fetchID")) {
        val fetchIdData = json("fetchID")
        val idList = processFetchID(fetchIdData, spark, pgConnectionProperties)
        println(idList.length)

        var df: DataFrame = spark.emptyDataFrame // Move df declaration outside the loop

        if (json.obj.contains("child")) {
          for (value <- idList) {
            val childData = json("child")
            // Assuming processChild returns a DataFrame
            df = processChild(childData.arr, spark, pgConnectionProperties, value)
            df = data_mapping_transformation_process(df, data_mapping, spark, pgConnectionProperties)
            println("lets check the output data schema")
            df = output_data_scheama_checking_process(df, output_mapping, spark, pgConnectionProperties)
            df.show()
            write_data_to_postgres(df, pgConnectionProperties)
          }
        }

        df // Return df outside the if statement
      } else {
        // If there is no "fetchID" key, you might want to handle this case accordingly.
        // For now, return an empty DataFrame.
        spark.emptyDataFrame
      }
    }


    def processFetchID(fetchID: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties): List[Int] = {
      try {
        val query = fetchID("query").str
        val databaseName = fetchID("database_name").str
        val id = fetchID("id").str
        val url = s"jdbc:postgresql://localhost:5432/$databaseName"

        //Print or process fetchID information
        println(s"Query: $query, Database: $databaseName, ID: $id, URL:$url")

        // Spark process to read data from JDBC source
        //TODO: changed the DB read query
        //val df: DataFrame = spark.read.jdbc(url, s"($query) + as subquery", pgConnectionProperties)
        val df: DataFrame = spark.read.format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/" + databaseName) // Replace with your PostgreSQL connection URL
          .option("dbtable", s"($query) as subquery")
          .option("user", dbUser) // Replace with your PostgreSQL username
          .option("password", dbPassword) // Replace with your PostgreSQL password
          .option("driver", "org.postgresql.Driver")
          .load()
        df.show()
        // Extract values from the DataFrame
        val idList: List[Int] = df.select(id).rdd.map(r => r.getInt(0)).collect().toList
        idList
      } catch {
        case e: Exception =>
          println(s"Error processing Spark DataFrame: ${e.getMessage}")
          List.empty[Int]
      }
    }


    def processChild(child: ujson.Arr, spark: SparkSession, pgConnectionProperties: Properties, value: Int): DataFrame = {
      var child_len: Int = child.arr.length
      var joinedDF: DataFrame = null
      println(child_len)
      println(child)
      for (j <- 0 until child_len) {
        if (j == 0) {
          println(child(0))
          if (child(0).obj.contains("single_process")) {
            val df = processSingleProcess(child(0)("single_process"), spark, pgConnectionProperties, value)
            joinedDF = df
          }
        } else {
          println(child(j))
          if (child(j).obj.contains("single_process")) {
            var df2 = processSingleProcess(child(j)("single_process"), spark, pgConnectionProperties, value)
            val join_on = child(j)("single_process")("join_on").str
            val join_type = child(j)("single_process")("join_type").str
            val agg_type = child(j)("single_process")("agg").str
            val agg_on = child(j)("single_process")("agg_on").str
            val rename = child(j)("single_process")("rename").str
            val groupby = child(j)("single_process")("groupby").str

            println("before_df2")
            df2.show()

            if (agg_type != "none" & agg_on != "none" & groupby != "none") {
              df2 = df2.groupBy(col(s"$groupby")).agg(expr(s"$agg_type($agg_on)").alias(rename))
              println("after_df2")
              df2.show()
            }

            joinedDF = df2.join(joinedDF, Seq(join_on), join_type)
            joinedDF.show()

          } else if (child(j).obj.contains("fetchID_with_InputID")) {
            val idList2: List[Int] = processFetchID_with_InputID(child(j)("fetchID_with_InputID"), spark, pgConnectionProperties, value)
            val join_on2 = child(j)("fetchID_with_InputID")("join_on").str
            val join_type2 = child(j)("fetchID_with_InputID")("join_type").str
            println(idList2)
            if (child(j).obj.contains("child")) {
              val child_data2 = child(j)("child").arr
              println(child_data2)
              var df3 = processChildWithMultipleProcess(child_data2, spark, pgConnectionProperties, idList2, value)
              joinedDF = df3.join(joinedDF, Seq(join_on2), join_type2)
            }
          }
        }
      }
      joinedDF
    }

    def data_mapping_transformation_process(df: DataFrame, data_mapping: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties): DataFrame = {
      println("Here we are printing data_mapping schema")
      println(data_mapping)
      var modified_df = df
      data_mapping.obj.keys.foreach { column_name =>
        println(column_name)
        if (modified_df.columns.contains(column_name)) {
          println("column present in the dataframe")
        } else {
          val aggregationValue = data_mapping(column_name)("aggregation")
          if (aggregationValue.isInstanceOf[ujson.Obj]) {
            // Handle the case where "aggregation" is a JSON object
            val aggType = aggregationValue("agg_type").str
            if (aggType == "average") {
              val column_arr = aggregationValue("column_name").arr
              val column1 = column_arr(0).str
              val column2 = column_arr(1).str
              modified_df = modified_df.withColumn(s"$column_name", round((col(column1) + col(column2)) / 2, 2))
            }
          } else if (aggregationValue.str == "none") {
            // Handle the case where "aggregation" is set to "none"
            println(s"Aggregation is set to 'none' for column: $column_name")
          }
        }
      }
      modified_df
    }


    def output_data_scheama_checking_process(df: DataFrame, output_mapping: ujson.Obj, spark: SparkSession, pgConnectionProperties: Properties): DataFrame = {

      // Fetch keys from the dictionary
      val keys = output_mapping.obj.keys.toSeq
      println(keys)
      println("inside output_data_scheama_checking_process method")

      // Select columns from the original DataFrame based on the keys
      var selectedColumnsDF = df.select(keys.map(col): _*)
      println(selectedColumnsDF)

      output_mapping.obj.keys.foreach { column_name =>
        println(column_name)
        println(selectedColumnsDF.schema(column_name).dataType)
        println(output_mapping(column_name)("Input_data_type").str)
        if ((selectedColumnsDF.schema(column_name).dataType).toString == output_mapping(column_name)("Input_data_type").str) {
          if (output_mapping(column_name)("Input_data_type").str == "ArrayType(StringType,true)") {
            selectedColumnsDF = selectedColumnsDF.withColumn(column_name, concat_ws(", ", col(column_name)))
          }
          selectedColumnsDF = selectedColumnsDF.withColumnRenamed(column_name, output_mapping(column_name)("rename").str)
        } else {
          println("data_type_not_matches")
        }
      }
      selectedColumnsDF
    }

    def write_data_to_postgres(df: DataFrame, pgConnectionProperties: Properties): Unit = {
      df.write
        .mode("append") // Change to "append" if needed
        .option("driver", "org.postgresql.Driver")
        .jdbc("jdbc:postgresql://localhost:5432/elevate_sink_data", "mentor_report_sink", pgConnectionProperties)
    }


    /**
     * After PROCESSJSON method
     */
    def processSingleProcess(singleProcessData: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties, value: Int): DataFrame = {

      try {
        val input = singleProcessData("input").str
        val query = singleProcessData("query").str
        val databaseName = singleProcessData("database_name").str
        val url = s"jdbc:postgresql://localhost:5432/$databaseName"
        val agg = singleProcessData("agg").str
        val agg_on = singleProcessData("agg_on").str

        var df: DataFrame = spark.emptyDataFrame
        // Replace placeholders in the query
        val substitutedQuery = query.replace("${id}", value.toString)

        // Process single input data
        println(s"Single Process: Input: $input, Query: $query, Database: $databaseName,URL:$url,substitutedQuery: $substitutedQuery")

        // Spark process to read data from JDBC source
        if (agg != "none" & agg_on != "none") {
          //TODO: changed the DB read query
          //df = spark.read.jdbc(url, s"($substitutedQuery)", pgConnectionProperties).na.fill(0).withColumn(agg_on, col(agg_on).cast("integer"))

          df = spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/" + databaseName) // Replace with your PostgreSQL connection URL
            .option("dbtable", s"($substitutedQuery) as subquery")
            .option("user", dbUser) // Replace with your PostgreSQL username
            .option("password", dbPassword) // Replace with your PostgreSQL password
            .option("driver", "org.postgresql.Driver")
            .load().na.fill(0).withColumn(agg_on, col(agg_on).cast("integer"))

        }
        else {

          //df = spark.read.jdbc(url, s"($substitutedQuery)", pgConnectionProperties).na.fill(0)
          df = spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/" + databaseName) // Replace with your PostgreSQL connection URL
            .option("dbtable", s"($substitutedQuery) as subquery")
            .option("user", dbUser) // Replace with your PostgreSQL username
            .option("password", dbPassword) // Replace with your PostgreSQL password
            .option("driver", "org.postgresql.Driver")
            .load().na.fill(0)
        }
        if (df.count == 0) {
          // Create a new DataFrame with a single column "response" and value 0
          val newSchema = StructType(Array(StructField(agg_on, IntegerType, true: Boolean)))
          val newData = Seq(Row(0))
          val newRdd = spark.sparkContext.parallelize(newData)
          df = spark.createDataFrame(newRdd, newSchema)

        } else {
          println(s"printing df :")
          df.show()
        }
        df = df.withColumn(input, lit(value))
        df.show()
        df

      } catch {
        case e: Exception =>
          println(s"Error processing Spark DataFrame: ${e.getMessage}")
          spark.emptyDataFrame
      }
    }


    def processFetchID_with_InputID(FetchID_with_InputID: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties, value: Int): List[Int] = {
      try {
        val query = FetchID_with_InputID("query").str
        val databaseName = FetchID_with_InputID("database_name").str
        val url = s"jdbc:postgresql://localhost:5432/$databaseName"
        val select_id = FetchID_with_InputID("id").str

        // Replace placeholders in the query
        val substitutedQuery = query.replace("${id}", value.toString)

        //Print or process fetchID information
        println(s"Query: $query, Database: $databaseName, URL:$url , SubstitutedQuery:$substitutedQuery")

        // Spark process to read data from JDBC source
        //val df: DataFrame = spark.read.jdbc(url, s"($substitutedQuery)", pgConnectionProperties)
        val df: DataFrame = spark.read.format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/" + databaseName) // Replace with your PostgreSQL connection URL
          .option("dbtable", s"($substitutedQuery) as subquery")
          .option("user", dbUser) // Replace with your PostgreSQL username
          .option("password", dbPassword) // Replace with your PostgreSQL password
          .option("driver", "org.postgresql.Driver")
          .load()
        df.show()

        // Extract values from the DataFrame
        val idList: List[Int] = df.select(select_id).rdd.map(r => r.getInt(0)).collect().toList
        idList
      } catch {
        case e: Exception =>
          println(s"Error processing Spark DataFrame: ${e.getMessage}")
          List.empty[Int]
      }
    }

    def processChildWithMultipleProcess(child: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties, idList2: List[Int], value: Int): DataFrame = {
      val childLen = child.arr.length
      println(s"childLen:$childLen")
      var joinedDF: DataFrame = null

      for (j <- 0 until childLen) {
        if (j == 0) {
          if (child(j).obj.contains("multiple_process")) {
            var df = processMultipleProcess(child(j)("multiple_process"), spark, pgConnectionProperties, idList2, value)
            joinedDF = df
          }
        } else if (child(j).obj.contains("multiple_process")) {
          val df = processMultipleProcess(child(j)("multiple_process"), spark, pgConnectionProperties, idList2, value)
          val join_on = child(j)("multiple_process")("join_on").str
          val join_type = child(j)("multiple_process")("join_type").str
          joinedDF = df.join(joinedDF, Seq(join_on), join_type)
        }
      }

      joinedDF
    }

    def processMultipleProcess(multipleProcessData: ujson.Value, spark: SparkSession, pgConnectionProperties: Properties, idList: List[Int], value: Int): DataFrame = {
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

    println("Process completed")
    // Stop the Spark session
    spark.stop()
  }
}









object generic_script {
  def main(args: Array[String]): Unit = {
    // Check if the correct number of arguments are provided
    if (args.length != 2) {
      println("Usage: MySparkApp <table_name> <script_type>")
      System.exit(1)
    }

    val table_name = args(0)
    println(table_name)
    val script_type = args(1)
    println(script_type)

    // Create an instance of MySparkApp with the provided arguments
    val app = new generic_script(table_name, script_type)
    // Run the application
    app.run()

  }
}

