import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, current_date, date_sub, initcap, sum, when}

object code10 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    /*Classify employees for a bonus eligibility program. Employees in "Sales" and "Marketing" with
performance scores above 80 get a 20% bonus, while others with scores above 70 get 15%. All other
employees receive no bonus. Group by department and calculate total bonus allocation*/

    val employees = List(
      ("karthik", "Sales", 85),
      ("neha", "Marketing", 78),
      ("priya", "IT", 90),
      ("mohan", "Finance", 65),
      ("ajay", "Sales", 55),
      ("vijay", "Marketing", 82),
      ("veer", "HR", 72),
      ("aatish", "Sales", 88),
      ("animesh", "Finance", 95),
      ("nishad", "IT", 60)).toDF("name", "department", "performance_score")

    /*    /*Classify employees for a bonus eligibility program. Employees in "Sales" and "Marketing" with
performance scores above 80 get a 20% bonus, while others with scores above 70 get 15%. All other
employees receive no bonus. Group by department and calculate total bonus allocation*/
*/

      employees.withColumn("bonus",
        when(col("department").isin("sales","Marketing") && col("performance_score")>=80,20).when(col("performance_score")>70,15).otherwise(0)).groupBy("department").agg(sum("bonus")).show()











  }
}