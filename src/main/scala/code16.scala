import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, current_date, date_sub, initcap, sum, when,min,max}

object code16 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    val electricityUsage = List(
      ("House1", 550, 250),
      ("House2", 400, 180),
      ("House3", 150, 50),
      ("House4", 500, 200),
      ("House5", 600, 220),
      ("House6", 350, 120),
      ("House7", 100, 30),
      ("House8", 480, 190),
      ("House9", 220, 105),
      ("House10", 150, 60)
    ).toDF("household", "kwh_usage", "total_bill")

    /* Classify households into "High Usage" if kWh exceeds 500 and bill exceeds $200,
"Medium Usage" for kWh between 200-500 and bill between $100-$200, and "Low Usage"
otherwise. Calculate the total number of households in each usage category.
8. Find the maximum bill amount for "High Usage" households and calculate the average kWh
for "Medium Usage" households.
9. Identify households with "Low Usage" but kWh usage exceeding 300. Count such
households*/

    val usagelist = electricityUsage.withColumn("category",
      when(col("kwh_usage") > 500 && col("total_bill") > 200, "HighUsage").
        when(col("kwh_usage").between(200, 500) && col("total_bill").between(100, 200), "MediumUsage").
        otherwise("low_usage"))

    usagelist.groupBy("category").agg(count(col("category"))).show()

    val high_usage= usagelist.filter(col("category")==="HighUsage").groupBy("category").agg(max("total_bill"))
    high_usage.show()
    usagelist.filter(col("category")==="MediumUsage").groupBy("category").agg(avg("kwh_usage")).show()
    usagelist.filter(col("category")==="low_usage" && col("kwh_usage")>300).groupBy("category").agg(count("category")).show()



  }
}

