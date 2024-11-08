import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, sum}

object prooblem3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("problem3").master("local[*]").getOrCreate()
    import spark.implicits._
    val orderData = List(("Order1","John",100),("Order2","Alice",200),("Order3", "Bob", 150), ("Order4", "Alice", 300),  ("Order5", "Bob", 250), ("Order6", "harika", 400)).toDF("OrderID", "Customer", "Amount")
    orderData.groupBy("customer").agg(count(col("OrderID")),sum(col("Amount"))).show()
  }
}
