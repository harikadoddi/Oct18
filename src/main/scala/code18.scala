import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, current_date, date_sub, initcap, sum, when,min,max}

object code18 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    val productSales = List(
      ("Product1", 250000, 5),
      ("Product2", 150000, 8),
      ("Product3", 50000, 20),
      ("Product4", 120000, 10),
      ("Product5", 300000, 7),
      ("Product6", 60000, 18),
      ("Product7", 180000, 9),
      ("Product8", 45000, 25),
      ("Product9", 70000, 15),
      ("Product10", 10000, 30)
    ).toDF("product_name", "total_sales", "discount")

    /*1. Classify products as "Top Seller" if total sales exceed 200,000 and discount offered is less
than 10%, "Moderate Seller" if total sales are between 100,000 and 200,000, and "Low
Seller" otherwise. Count the total number of products in each classification.
2. Find the maximum sales value among "Top Seller" products and the minimum discount rate
among "Moderate Seller" products.
3. Identify products from the "Low Seller" category with a total sales value below 50,000 and
discount offered above 15%*/

    val categorylist = productSales.withColumn("category",
      when(col("total_sales")>200000 && col("discount")<10 , "Top Seller").
        when(col("total_sales").between(100000,200000),"Moderate Seller").otherwise("LowSeller"))
    categorylist.groupBy("category").agg(count("category")).show
    categorylist.filter(col("category")==="Top Seller").groupBy("category").agg(max("total_sales")).show()
    categorylist.filter(col("category")==="Moderate Seller").groupBy("category").agg(min("discount")).show()
    categorylist.filter(col("category")==="LowSeller" && col("total_sales")<50000 && col("discount")>15).show()
  }
}