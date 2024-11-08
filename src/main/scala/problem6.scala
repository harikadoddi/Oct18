import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, min,max}

object problem6 {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().appName("problem6").master("local[*]").getOrCreate()
    import spark.implicits._
    val weatherData = Seq(
      ("City1", "2022-01-01", 10.0),
      ("City1", "2022-01-02", 8.5),
      ("City1", "2022-01-03", 12.3),
      ("City2", "2022-01-01", 15.2),
      ("City2", "2022-01-02", 14.1),
      ("City2", "2022-01-03", 16.8)
    ).toDF("City", "Date", "Temperature")

    //.Finding the minimum, maximum, and average temperature for each city in a weather dataset.

    weatherData.groupBy("city").agg(min(col("temperature")),max(col("temperature")),avg(col("temperature"))).show()

  }

}
