import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, date_sub, initcap, when}

object code1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    /*Seekho Bigdata Institute
© 2024 Seekho Bigdata Institute. All rights reserved. www.seekhobigdata.com 9989454737
Question 1: Employee Status Check
Create a DataFrame that lists employees with names and their work status. For each employee,
determine if they are “Active” or “Inactive” based on the last check-in date. If the check-in date is
within the last 7 days, mark them as "Active"; otherwise, mark them as "Inactive." Ensure the first
letter of each name is capitalized*/

    val employees = List(
      ("karthik", "2024-11-01"),
      ("neha", "2024-10-20"),
      ("priya", "2024-10-28"),
      ("mohan", "2024-11-02"),
      ("ajay", "2024-09-15"),
      ("vijay", "2024-10-30"),
      ("veer", "2024-10-25"),
      ("aatish", "2024-10-10"),
      ("animesh", "2024-10-15"),
      ("nishad", "2024-11-01"),
      ("varun", "2024-10-05"),
      ("aadil", "2024-09-30")
    ).toDF("name", "last_checkin")

    val currentDate = current_date()
    val dateSevenDaysAgo = date_sub(currentDate, 7)

    employees.withColumn("name", initcap(col("name"))).withColumn("status", when(col("last_checkin").between(dateSevenDaysAgo,currentDate),"ACTIVE").otherwise("InActive")).show()

  }

}
