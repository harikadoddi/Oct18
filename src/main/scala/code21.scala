import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, current_date, date_sub, initcap, sum, when,min,max}

object code21{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    val employeeProductivity = List(
      ("Emp1", 85, 6),
      ("Emp2", 75, 4),
      ("Emp3", 40, 1),
      ("Emp4", 78, 5),
      ("Emp5", 90, 7),
      ("Emp6", 55, 3),
      ("Emp7", 80, 5),
      ("Emp8", 42, 2),
      ("Emp9", 30, 1),
      ("Emp10", 68, 4)
    ).toDF("employee_id", "productivity_score", "project_count")

    /*Classify employees as "High Performer" if productivity score > 80 and project count
      is greater than 5, "Average Performer" if productivity score is between 60 and 80, and "Low
    Performer" otherwise. Count employees in each classification.
    11. Calculate the average productivity score for "High Performer" employees and the minimum
    score for "Average Performers."
    12. Identify "Low Performers" with a productivity score below 50 and project count under 2*/

    val categorywise = employeeProductivity.withColumn("category",
      when(col("productivity_score")>80 && col("project_count")>5 , "High Performer").
        when(col("productivity_score").between(60,80),"Average Performer").otherwise("Low Performer"))
    categorywise.show()
    categorywise.filter(col("category")==="High Performer").groupBy("category").agg(avg(col("productivity_score"))).show()
    categorywise.filter(col("category")==="Average Performer").groupBy("category").agg(min(col("productivity_score"))).show()
    categorywise.filter(col("category")==="Low Performer").filter(col("productivity_score")<50 && col("project_count")<2).show()


  }
}