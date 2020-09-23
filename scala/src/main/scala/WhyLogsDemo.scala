import java.time.LocalDateTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.whylogs.spark.WhyLogs._

object WhyLogsDemo extends App {

  val spark = SparkSession
    .builder()
    .master("local[*, 3]")
    .appName("SparkTesting-" + LocalDateTime.now().toString)
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  val raw_df = spark.read
    .option("header", "true")
    .csv("Fire_Department_Calls_for_Service.csv")

  val df =
    raw_df.withColumn("call_date", to_timestamp(col("Call Date"), "MM/dd/YYYY"))
  df.printSchema()

  val profiles = df
    .newProfilingSession("FireDepartment") // start a new WhyLogs profiling job
    .withTimeColumn("call_date") // split dataset by call_date
    .groupBy("City", "Priority") // tag and group the data with categorical information
    .aggProfiles() //  runs the aggregation. returns a dataframe of <timestamp, datasetProfile> entries

  profiles.write
    .mode(SaveMode.Overwrite)
    .parquet("profiles_parquet")

}
