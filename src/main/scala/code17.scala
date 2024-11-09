import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, current_date, date_sub, initcap, sum, when,min,max}

object code17 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    val employees = List(
      ("karthik", "IT", 110000, 12, 88),
      ("neha", "Finance", 75000, 8, 70),
      ("priya", "IT", 50000, 5, 65),
      ("mohan", "HR", 120000, 15, 92),
      ("ajay", "IT", 45000, 3, 50),
      ("vijay", "Finance", 80000, 7, 78),
      ("veer", "Marketing", 95000, 6, 85),
      ("aatish", "HR", 100000, 9, 82),
      ("animesh", "Finance", 105000, 11, 88),
      ("nishad", "IT", 30000, 2, 55)
    ).toDF("name", "department", "salary", "experience", "performance_score")


    /*Classify employees into salary bands: "Senior" if salary > 100k and experience > 10
years, "Mid-level" if salary between 50-100k and experience 5-10 years, and "Junior" otherwise.
Group by department to find count of each salary band.
Seekho Bigdata Institute
© 2024 Seekho Bigdata Institute. All rights reserved. www.seekhobigdata.com 9989454737
11. For each salary band, calculate the average performance score. Filter for bands where
average performance exceeds 80.
12. Find employees in "Mid-level" band with performance above 85 and experience over 7 years. */

    val bands= employees.withColumn("salaryband",
      when(col("salary")>100000 && col("experience")>10, "Senior")
    .when(col("salary").between(50000,100000) && col("experience").between(5,10),"Mid-level").
        otherwise("Junior"))
    bands.show()
    bands.groupBy("department").agg(count("salaryband")).show()
    bands.groupBy("salaryband").agg(avg("performance_score").alias("avgscore")).filter(col("avgscore")>80).show()
    bands.filter(col("salaryband")==="Mid-level" && col("performance_score")>85 && col("experience")>7).show()

  }
}

