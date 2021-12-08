// Tested on Databricks cluster running as scala notebook:
// * cluster version: 8.3 (includes Apache Spark 3.1.1, Scala 2.12)
// * installed whylogs jar: whylogs_spark_bundle_3_1_1_scala_2_12_0_1_21aeb7b2_20210903_224257_1_all-d1b20.jar
// * from: https://oss.sonatype.org/content/repositories/snapshots/ai/whylabs/whylogs-spark-bundle_3.1.1-scala_2.12/0.1-21aeb7b2-SNAPSHOT/whylogs-spark-bundle_3.1.1-scala_2.12-0.1-21aeb7b2-20210903.224257-1-all.jar

import java.time.LocalDateTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.whylogs.spark.WhyLogs._

// COMMAND ----------

// For demo purposes we will create a time column with yesterday's date, so that Whylabs ingestion sees this as a recent dataset profile
// and it shows up in default dashboard of last 7 days on Whylabs.
def unixEpochTimeForNumberOfDaysAgo(numDaysAgo: Int): Long = {
    import java.time._
    val numDaysAgoDateTime: LocalDateTime = LocalDateTime.now().minusDays(numDaysAgo)
    val zdt: ZonedDateTime = numDaysAgoDateTime.atZone(ZoneId.of("America/Los_Angeles"))
    val numDaysAgoDateTimeInMillis = zdt.toInstant.toEpochMilli
    val unixEpochTime = numDaysAgoDateTimeInMillis / 1000L
    unixEpochTime
}

val timestamp_yesterday = unixEpochTimeForNumberOfDaysAgo(1)
println(timestamp_yesterday)
val timeColumn = "dataset_timestamp"


// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
val spark = SparkSession
  .builder()
  .master("local[*, 3]")
  .appName("SparkTesting-" + LocalDateTime.now().toString)
  .config("spark.ui.enabled", "false")
  .getOrCreate()

// the file location below is using the lending_club_1000.csv uploaded onto a Databricks dbfs 
//   e.g. from here https://github.com/whylabs/whylogs/blob/mainline/testdata/lending_club_1000.csv
// you will need to update that location based on a dataset you use or follow this example.
val input_dataset_location = "dbfs:/FileStore/tables/lending_club_1000.csv"

val raw_df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(input_dataset_location)

// Here we add an artificial column for time. It is required that there is a TimestampType column for profiling with this API
val df = raw_df.withColumn(timeColumn, lit(timestamp_yesterday).cast(DataTypes.TimestampType))
df.printSchema()


// COMMAND ----------

val session = df.newProfilingSession("LendingClubScala") // start a new WhyLogs profiling job
  .withTimeColumn(timeColumn)
val profiles = session
  .aggProfiles() //  runs the aggregation. returns a dataframe of <dataset_timestamp, datasetProfile> entries

// COMMAND ----------

// optionally you might write the dataset profiles out somewhere before uploading to Whylabs
profiles.write
    .mode(SaveMode.Overwrite)
    .parquet("dbfs:/FileStore/tables/whylogs_demo_profiles_parquet")

// COMMAND ----------

// Replace the following parameters below with your values after signing up for an account at https://whylabs.ai/
// You can find Organization Id on https://hub.whylabsapp.com/settings/access-tokens and the value looks something like: org-123abc
// also the settings page allows you t create new apiKeys which you will need an apiKey to upload to your account in Whylabs
// The modelId below specifies which model this profile is for, by default an initial model-1 is created but you will update this
// if you create a new model here https://hub.whylabsapp.com/settings/model-management
session.log(
          orgId = "replace_with_your_orgId",
          modelId = "model-1",
          apiKey = "replace_with_your_api_key")

