import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, current_date, date_sub, initcap, sum, when,min}

object code15 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    val customerPurchases = List(
      ("karthik", "Premium", 50, 5000),
      ("neha", "Standard", 10, 2000),
      ("priya", "Premium", 65, 8000),
      ("mohan", "Basic", 90, 1200),
      ("ajay", "Standard", 25, 3500),
      ("vijay", "Premium", 15, 7000),
      ("veer", "Basic", 75, 1500),
      ("aatish", "Standard", 45, 3000),
      ("animesh", "Premium", 20, 9000),
      ("nishad", "Basic", 80, 1100)
    ).toDF("name", "membership", "days_since_last_purchase", "total_purchase_amount")


    var newdf = customerPurchases.withColumn("category",
      when(col("days_since_last_purchase")<30,"Frequent").
        when(col("days_since_last_purchase").between(30,60),"Occasional").
        otherwise("Rare"))
    newdf.show()
    val categorywisedetails = newdf.groupBy("category").agg(count(col("category")))
    categorywisedetails.show()

    val avgamount = newdf.filter(col("category")=== "Frequent" && col("membership") === "Premium").groupBy("category").agg(avg(col("total_purchase_amount")))
    avgamount.show()

    val minamount = newdf.filter(col("category")==="Rare").groupBy("category").agg(min("total_purchase_amount"))
    minamount.show()





    /*Scenario 15: Customer Purchase Recency Categorization
Question Set: 4. Categorize customers based on purchase recency: "Frequent" if last purchase within
30 days, "Occasional" if within 60 days, and "Rare" if over 60 days. Show the number of each
category per membership type.
5. Find the average total purchase amount for customers with "Frequent" purchase recency
and "Premium" membership.
6. For customers with "Rare" recency, calculate the minimum purchase amount across different
membership types*/

//    val current_Date = current_date()
//    val date_thirty = date_sub(current_Date, 30)






  }
}

