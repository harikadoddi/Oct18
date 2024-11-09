import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, current_date, date_sub, initcap, sum, when}

object code9 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    /*Classify inventory stock levels as "Overstocked" if stock exceeds 100, "Normal" if between 50-100,
and "Low Stock" if below 50. Aggregate total stock in each category*/

    val inventory = List(
      ("ProductA", 120),
      ("ProductB", 95),
      ("ProductC", 45),
      ("ProductD", 200),
      ("ProductE", 75),
      ("ProductF", 30),
      ("ProductG", 85),
      ("ProductH", 100),
      ("ProductI", 60),
      ("ProductJ", 20)
    ).toDF("product_name", "stock_quantity")

    inventory.withColumn("category", when(col("stock_quantity")>100 ,"Overstocked").when(col("stock_quantity").between(50,100),"Normal")
    .otherwise("low Stock")).groupBy("category").agg(count("category")).show()


  }
}