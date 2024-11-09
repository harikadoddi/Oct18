import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, current_date, date_sub, initcap, sum, when,min,max}

object code24 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    val students = List(
      ("Student1", 70, 45, 60, 65, 75),
      ("Student2", 80, 55, 58, 62, 67),
      ("Student3", 65, 30, 45, 70, 55),
      ("Student4", 90, 85, 80, 78, 76),
      ("Student5", 72, 40, 50, 48, 52),
      ("Student6", 88, 60, 72, 70, 68),
      ("Student7", 74, 48, 62, 66, 70),
      ("Student8", 82, 56, 64, 60, 66),
      ("Student9", 78, 50, 48, 58, 55),
      ("Student10", 68, 35, 42, 52, 45)
    ).toDF("student_id", "attendance_percentage", "math_score", "science_score", "english_score",
      "history_score")

      /*Classify students as "At-Risk" if attendance is below 75% and the average test score
is below 50, "Moderate Risk" if attendance is between 75% and 85%, and "Low Risk" otherwise.
Calculate the number of students in each risk category.
8. Find the average score for students in the "At-Risk" category.
9. Identify "Moderate Risk" students who have scored above 70 in at least three subjects*/

    val avgscore = students.withColumn("avg_testscore", (col("math_score")+col("science_score")+col("english_score")+col("history_score"))/4.0)
    val categorywise = avgscore.withColumn("category",
      when(col("attendance_percentage")<75 && col("avg_testscore")<50 ,"At-Risk").
        when(col("attendance_percentage").between(75,85), "Moderate Risk").otherwise("Low Risk"))
    categorywise.show()
    categorywise.groupBy("category").agg(count("category")).show()
    categorywise.filter(col("category")==="At-Risk").groupBy("category").agg(avg(col("avg_testscore"))).show()
    categorywise.filter(col("category") === "Moderate Risk").withColumn ("highscore", (when(col("math_score")>70,1).otherwise(0))+ (when(col("science_score")>70,1).otherwise(0)) + (when(col("english_score")>70,1).otherwise(0)) + (when(col("history_score")>70,1).otherwise(0))).
      filter(col("highscore")>=3).show()
  }
}

