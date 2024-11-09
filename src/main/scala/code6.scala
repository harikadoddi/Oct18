import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, current_date, date_sub, initcap, sum, when}

object code6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    /*Group customers as "Youth" if under 25, "Adult" if between 25-45, and "Senior" if over 45. Capitalize
names and show total customers in each group.*/

    val customers = List(
      ("karthik", 22),
      ("neha", 28),
      ("priya", 40),
      ("mohan", 55),
      ("ajay", 32),
      ("vijay", 18),
      ("veer", 47),
      ("aatish", 38),
      ("animesh", 60),
      ("nishad", 25)
    ).toDF("name", "age")

    customers.withColumn("name",initcap(col("name"))).withColumn("group", when(col("age")>45,"Senior").when(col("age").between(25,45),"Adult").otherwise("Youth")).groupBy("group").agg(count(col("group"))).show()


  }
}