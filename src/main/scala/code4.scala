import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, current_date, date_sub, initcap, sum, when}

object code4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    /*
    Determine whether an employee has "Excessive Overtime" if their weekly hours exceed 60,
"Standard Overtime" if between 45-60 hours, and "No Overtime" if below 45 hours. Capitalize each
name and group by overtime status
     */
    val employees = List(
      ("karthik", 62),
      ("neha", 50),
      ("priya", 30),
      ("mohan", 65),
      ("ajay", 40),
      ("vijay", 47),
      ("veer", 55),
      ("aatish", 30),
      ("animesh", 75),
      ("nishad", 60)
    ).toDF("name", "hours_worked")

    employees.withColumn("name",initcap(col("name"))).withColumn("staus", when(col("hours_worked")>60,"Excessive Overtime").when(col("hours_worked").between(40,60),"Standard Overtime").otherwise("No Overtime")).show(
    )

  }
}