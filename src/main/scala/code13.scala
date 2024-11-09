//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{avg, col, count, current_date, date_sub, initcap, sum, when}
//
//object code13 {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
//    import spark.implicits._
//
//    val orders = List(
//      ("Order1", "Laptop", "Domestic", 2),
//      ("Order2", "Shoes", "International", 8),
//      ("Order3", "Smartphone", "Domestic", 3),
//      ("Order4", "Tablet", "International", 5),
//      ("Order5", "Watch", "Domestic", 7),
//      ("Order6", "Headphones", "International", 10),
//      ("Order7", "Camera", "Domestic", 1),
//      ("Order8", "Shoes", "International", 9),
//      ("Order9", "Laptop", "Domestic", 6),
//      ("Order10", "Tablet", "International", 4)
//    ).toDF("order_id", "product_type", "origin", "delivery_days")
//
//    /*Classify orders as "Delayed" if delivery time exceeds 7 days and origin location is "International",
//    "On-Time" if between 3-7 days, and "Fast" if below 3 days. Group by product type to see the count of
//      each delivery speed category*/
//
//     orders.withColumn("category",
//       when(col("delivery_days")>7 && col("origin").equals("International"),"exceeds").
//         when(col("delivery_days").between(3,7),"On-time").otherwise("Fast")).
//       groupBy("product_type").agg(count("category")).show()
//
//  }
//}
//
