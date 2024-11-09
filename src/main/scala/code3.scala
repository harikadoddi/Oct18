import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, current_date, date_sub, initcap, sum, when}

object code3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    /* Given a DataFrame with project allocation data for multiple employees, determine each employee's
workload level based on their hours worked in a month across various projects. Categorize
employees as “Overloaded” if they work more than 200 hours, “Balanced” if between 100-200 hours,
and “Underutilized” if below 100 hours. Capitalize each employee’s name, and show the aggregated
workload status count by category */

    val workload = List(
      ("karthik", "ProjectA", 120),
      ("karthik", "ProjectB", 100),
      ("neha", "ProjectC", 80),
      ("neha", "ProjectD", 30),
      ("priya", "ProjectE", 110),
      ("mohan", "ProjectF", 40),
      ("ajay", "ProjectG", 70),
      ("vijay", "ProjectH", 150),
      ("veer", "ProjectI", 190),
      ("aatish", "ProjectJ", 60),
      ("animesh", "ProjectK", 95),
      ("nishad", "ProjectL", 210),
      ("varun", "ProjectM", 50),
      ("aadil", "ProjectN", 90)
      ).toDF("name", "project", "hours")

    workload.withColumn("name",initcap(col("name"))).groupBy("name").agg(sum(col("hours")).alias("hours")).withColumn("category",
      when(col("hours")>200,"Overloaded").when(col("hours")<=200 && col("hours")>=100,"Balanced").otherwise("Underutilized")).groupBy("category").agg(count(col("category"))).show()
  }
}