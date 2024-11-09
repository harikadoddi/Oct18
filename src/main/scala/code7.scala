import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, current_date, date_sub, initcap, sum, when}

object code7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    /*Classify each vehicle’s mileage as "High Efficiency" if mileage is above 25 MPG, "Moderate Efficiency"
if between 15-25 MPG, and "Low Efficiency" if below 15 MPG*/

    val vehicles = List(
      ("CarA", 30),
      ("CarB", 22),
      ("CarC", 18),
      ("CarD", 15),
      ("CarE", 10),
      ("CarF", 28),
      ("CarG", 12),
      ("CarH", 35),
      ("CarI", 25),
      ("CarJ", 16)
    ).toDF("vehicle_name", "mileage")

    vehicles.withColumn("status", when(col("mileage")>25,"High Efficiency").when(col("mileage").between(15,25),"Moderate Efficiency").otherwise("low_efficiency")).orderBy("status")show()


  }
}
