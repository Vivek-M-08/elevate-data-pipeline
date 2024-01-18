package org.shikshalokam.job.mentoring.task

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.shikshalokam.job.mentoring.functions
import org.codehaus.jackson.map.MapperConfig.Base
import org.shikshalokam.job.mentoring.functions.mentoringFunction

import java.util.Properties

class mentoringExecution(table_name: String, script_type: String) extends mentoringFunction {

  def process(): Unit = {

    val configProperties: Properties = new Properties()
    configProperties.load(getClass.getResourceAsStream("/application.properties"))
    val dbUser: String = configProperties.getProperty("db.user")
    val dbPassword: String = configProperties.getProperty("db.password")
    val inputDatabase: String = configProperties.getProperty("input.database")
    val url: String = configProperties.getProperty("url")
    val query = s"SELECT * FROM $table_name WHERE script_type = '$script_type'"

    val spark = SparkSession.builder()
      .appName("elevate_generic_jobs")
      .config("spark.master", "local[2]")
      .getOrCreate()



    val config_df: DataFrame = readFromPostgres(url, inputDatabase, query, dbUser, dbPassword, spark)
    //Todo remove bellow print statement
    println(s"${url}/$inputDatabase")
    println(query)
    config_df.show(false)

    val input_data_schema = config_df.select("input_data_schema").as(Encoders.STRING).first()
    val output_data_schema = config_df.select("output_data_schema").as(Encoders.STRING).first()
    val data_mapping_schema = config_df.select("data_mapping").as(Encoders.STRING).first()
    //df.select("input_data_schema").asInstanceOf[String]
    println("input_data_schema as in string" + input_data_schema)
    println("output_data_schema as in string" + output_data_schema)
    println("data_mapping_schema as in string" + data_mapping_schema)
    println("\n\n")

    val input_json: ujson.Value = ujson.read(input_data_schema)
    val output_json: ujson.Value = ujson.read(output_data_schema)
    val data_mapping_json: ujson.Value = ujson.read(data_mapping_schema)
    println("input_json as in ujson value" + input_json)
    println("output_json as in ujson value" + output_json)
    println("data_mapping_json as in ujson value" + data_mapping_json)
    println("\n\n")

    // Process the JSON structure
    val inputData = input_json.arr
    val outputData = output_json.obj
    val MappingData = data_mapping_json.obj
    println(inputData)
    println(outputData)
    println(MappingData)

    //Todo rework : Chack and confirm is it necessary to pass this in for loop
    inputData.foreach { inputData =>
      println("+++++++++++++++++\n")
      println(inputData)
      processScriptLevelData(inputData, MappingData, outputData, spark: SparkSession)
    }
  }

}

object mentoringExecution {
  def main(args: Array[String]): Unit = {
    // Check if the correct number of arguments are provided
    if (args.length != 2) {
      println("Usage: MySparkApp <table_name> <script_type>")
      System.exit(1)
    }

    //TODO: Disabled the spark logging
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val table_name = args(0)
    println(table_name)
    val script_type = args(1)
    println(script_type)

    // Create an instance of MySparkApp with the provided arguments
    val app = new mentoringExecution(table_name, script_type)
    // Run the application
    app.process()

  }
}
