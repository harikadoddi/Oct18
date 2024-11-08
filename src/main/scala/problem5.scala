import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count}

object problem5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("program5").master("local[*]").getOrCreate()
    import spark.implicits._
    val ratingsData = List(
      ("User1", "Movie1", 4.5),
      ("User2", "Movie1", 3.5),
      ("User3", "Movie2", 2.5),
      ("User4", "Movie2", 3.0),
      ("User1", "Movie3", 5.0),
      ("User2", "Movie3", 4.0)
    ).toDF("User", "Movie", "Rating")

    ratingsData.groupBy("Movie").agg(avg(col("rating")),count(col("rating"))).show()
  }
}
