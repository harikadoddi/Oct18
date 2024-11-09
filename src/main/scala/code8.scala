import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, current_date, date_sub, initcap, sum, when}

object code8 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    /*Classify students based on their scores as "Excellent" if score is 90 or above, "Good" if between 75-
  89, and "Needs Improvement" if below 75. Count students in each category*/

    val students = List(
      ("karthik", 95),
      ("neha", 82),
      ("priya", 74),
      ("mohan", 91),
      ("ajay", 67),
      ("vijay", 80),
      ("veer", 85),
      ("aatish", 72),
      ("animesh", 90),
      ("nishad", 60)
    ).toDF("name", "score")

    students.withColumn("Category", when(col("score")>=90,"Excellent")
    .when(col("score").between(75,89),"good")
    .otherwise("Needs Improvement")).groupBy("category").agg(count("category")).show()



  }
}






