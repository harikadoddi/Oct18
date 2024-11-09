import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, date_sub, initcap, sum, when}

object code2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    /*Given a DataFrame of sales agents with their total sales amounts, calculate the performance status
based on sales thresholds: “Excellent” if sales are above 50,000, “Good” if between 25,000 and
50,000, and “Needs Improvement” if below 25,000. Capitalize each agent's name, and show total
sales aggregated by performance status*?

     */

    val sales = List(
      ("karthik", 60000),
      ("neha", 48000),
      ("priya", 30000),
      ("mohan", 24000),
      ("ajay", 52000),
      ("vijay", 45000),
      ("veer", 70000),
      ("aatish", 23000),
      ("animesh", 15000),
      ("nishad", 8000),
      ("varun", 29000),
      ("aadil", 32000)).toDF("name","total_sales")

    val df= sales.withColumn("name" , initcap(col("name"))).withColumn("performance_status",
      when(col("total_sales")>=50000,"Excellent").when(col("total_sales")>=25000 && col("total_sales")<50000,"good").otherwise("Needs improvement"))

    df.groupBy("performance_status").agg(sum(col("total_sales"))).show()
    df.show()
  }
}
