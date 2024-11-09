import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, current_date, date_sub, initcap, sum, when,min,max}

object code20{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._
    val ecommerceReturn = List(
      ("Product1", 75, 25),
      ("Product2", 40, 15),
      ("Product3", 30, 5),
      ("Product4", 60, 18),
      ("Product5", 100, 30),
      ("Product6", 45, 10),
      ("Product7", 80, 22),
      ("Product8", 35, 8),
      ("Product9", 25, 3),
      ("Product10", 90, 12)
      ).toDF("product_name", "sale_price", "return_rate")

    /*Classify products by return rate: "High Return" if return rate is over 20%, "Medium
    Return" if return rate is between 10% and 20%, and "Low Return" otherwise. Count products in each
    classification.
    8. Calculate the average sale price for "High Return" products and the maximum return rate for
      "Medium Return" products.
    9. Identify "Low Return" products with a sale price under 50 and return rate less than 5% */

    val categorywise = ecommerceReturn.withColumn("category",
      when(col("return_rate")>20 , "High Return").
        when(col("return_rate").between(10,20), "Medium Return").otherwise("Low Return"))
    categorywise.show()
    categorywise.filter(col("category")==="High Return").groupBy("category").agg(avg(col("sale_price"))).show()
    categorywise.filter(col("category")==="Medium Return").groupBy("category").agg(max(col("return_rate"))).show()
    categorywise.filter(col("category")==="Low Return").filter(col("sale_price")<50 && col("return_rate")<5).show()

  }
}