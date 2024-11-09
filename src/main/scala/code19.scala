import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, current_date, date_sub, initcap, sum, when,min,max}

object code19 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    val customerLoyalty = List(
      ("Customer1", 25, 700),
      ("Customer2", 15, 400),
      ("Customer3", 5, 50),
      ("Customer4", 18, 450),
      ("Customer5", 22, 600),
      ("Customer6", 2, 80),
      ("Customer7", 12, 300),
      ("Customer8", 6, 150),
      ("Customer9", 10, 200),
      ("Customer10", 1, 90)
    ).toDF("customer_name", "purchase_frequency", "average_spending")

    /*Question Set: 4. Classify customers as "Highly Loyal" if purchase frequency is greater than 20 times
and average spending is above 500, "Moderately Loyal" if frequency is between 10-20 times, and
"Low Loyalty" otherwise. Count customers in each classification.
5. Calculate the average spending of "Highly Loyal" customers and the minimum spending for
"Moderately Loyal" customers.
6. Identify "Low Loyalty" customers with an average spending less than 100 and purchase
frequency under 5*/

    val categorywise = customerLoyalty.withColumn("category",
      when(col("purchase_frequency")>20 && col("average_spending")>500 , "Highly Loyal").
        when(col("purchase_frequency").between(10,20),"Moderately Loyal").otherwise("Low Loyalty"))
    categorywise.show()
    categorywise.groupBy("category").agg(count("category")).show()
    categorywise.filter(col("category")==="Highly Loyal").groupBy("category").agg(avg(col("average_spending"))).show()
    categorywise.filter(col("category")==="Moderately Loyal").groupBy("category").agg(min(col("average_spending"))).show()
    categorywise.filter(col("category")==="Low Loyalty").filter(col("average_spending")<100 && col("purchase_frequency")<5).show()

  }
}