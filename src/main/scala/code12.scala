import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, current_date, date_sub, initcap, sum, when}

object code12{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    val customers = List(
      ("karthik", "Premium", 1050, 32),
      ("neha", "Standard", 800, 28),
      ("priya", "Premium", 1200, 40),
      ("mohan", "Basic", 300, 35),
      ("ajay", "Standard", 700, 25),
      ("vijay", "Premium", 500, 45),
      ("veer", "Basic", 450, 33),
      ("aatish", "Standard", 600, 29),
      ("animesh", "Premium", 1500, 60),
      ("nishad", "Basic", 200, 21)
    ).toDF("name", "membership", "spending", "age")

    /*code12 :
    Classify customers' spending as "High Spender" if spending exceeds $1000 with "Premium"
membership, "Average Spender" if spending between $500-$1000 and membership is "Standard",
and "Low Spender" otherwise. Group by membership and calculate average spending
     */

    customers.withColumn("category",
      when(col("spending") > 1000 && col("membership").equals("Premium"), "High Spender").when(col("spending").between(5000, 10000) && col("membership").equals("Standard"), "Average spender")
        .otherwise("low_spender")
    ).groupBy("category").agg(avg("spending")).show()
  }
}
