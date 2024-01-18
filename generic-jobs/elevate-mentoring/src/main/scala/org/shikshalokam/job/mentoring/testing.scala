package org.shikshalokam.job.mentoring

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

class testing {

  def run(): Unit = {

    println("Staring the app")


    val spark = SparkSession.builder()
      .appName("testing")
      .config("spark.master", "local") // Replace with your cluster configuration if not running locally
      .getOrCreate()

    val configProperties: Properties = new Properties()
    configProperties.load(getClass.getResourceAsStream("/application.properties"))

    val dbUser: String = configProperties.getProperty("db.user")
    val dbPassword: String = configProperties.getProperty("db.password")
    val inputDatabase: String = configProperties.getProperty("input.database")

    val query = s"SELECT * FROM config_table WHERE script_type = 'mentor_report' "
    println(query)

    Class.forName("org.postgresql.Driver")


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


    println("Hi")
  }
}

object testing {
  def main(args: Array[String]): Unit = {
    println("Before Testing class")
    val app = new testing
    app.run()

  }
}
