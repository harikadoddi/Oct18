import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col,max}

object problem4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("problem4").master("local[*]").getOrCreate()
    import spark.implicits._
    val scoreData = List(
      ("Alice", "Math", 80),
      ("Bob", "Math", 90),
      ("Alice", "Science", 70),
      ("Bob", "Science", 85),
      ("Alice", "English", 75),
      ("Bob", "English", 95)
    ).toDF("Student", "Subject", "Score")

    scoreData.groupBy("Student").agg(max(col("Score"))).show()
    scoreData.groupBy("Subject").agg(avg(col("Score"))).show()
  }

}
