import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, current_date, date_sub, initcap, sum, when}

object code11 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._
    val products = List(
      ("Laptop", "Electronics", 120, 45),
      ("Smartphone", "Electronics", 80, 60),
      ("Tablet", "Electronics", 50, 72),
      ("Headphones", "Accessories", 110, 47),
      ("Shoes", "Clothing", 90, 55),
      ("Jacket", "Clothing", 30, 80),
      ("TV", "Electronics", 150, 40),
      ("Watch", "Accessories", 60, 65),
      ("Pants", "Clothing", 25, 75),
      ("Camera", "Electronics", 95, 58)
      ).toDF("product_name", "category", "return_count", "satisfaction_score")

    /*For each product, classify return reasons as "High Return Rate" if return count exceeds 100 and
satisfaction score below 50, "Moderate Return Rate" if return count is between 50-100 with a score
between 50-70, and "Low Return Rate" otherwise. Group by category to count product return rates.*/

    products.withColumn("category",
      when(col("return_count")>100 && col("satisfaction_score")<50,"High Return Rate").when(col("return_count").between(50,100) && col("satisfaction_score").between(50,70),"Moderate Return Rate").otherwise("Low Return Rate")).groupBy("category").agg(count(col("category"))).show()
  }
}