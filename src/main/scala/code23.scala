import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, current_date, date_sub, initcap, sum, when,min,max}

object code23 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("code1").master("local[*]").getOrCreate()
    import spark.implicits._

    val patients = List(
      ("Patient1", 62, 10, 3, "ICU"),
      ("Patient2", 45, 25, 1, "General"),
      ("Patient3", 70, 8, 2, "ICU"),
      ("Patient4", 55, 18, 3, "ICU"),
      ("Patient5", 65, 30, 1, "General"),
      ("Patient6", 80, 12, 4, "ICU"),
      ("Patient7", 50, 40, 1, "General"),
      ("Patient8", 78, 15, 2, "ICU"),
      ("Patient9", 40, 35, 1, "General"),
      ("Patient10", 73, 14, 3, "ICU")
    ).toDF("patient_id", "age", "readmission_interval", "icu_admissions", "admission_type")

    /*Classify patients as "High Readmission Risk" if their last readmission interval (in
days) is less than 15 and their age is above 60, "Moderate Risk" if the interval is between 15 and 30
days, and "Low Risk" otherwise. Count patients in each category.
5. Find the average readmission interval for "High Readmission Risk" patients.
6. Identify "Moderate Risk" patients who were admitted to the "ICU" more than twice in the
past year*/

    val categorywise = patients.withColumn("category",
      when(col("readmission_interval")<15 && col("age")>60 , "High Readmission Risk").
        when(col("readmission_interval").between(15,30), "Moderate Risk").otherwise("Low Risk"))
    categorywise.show()

    categorywise.groupBy("category").agg(count("category")).show()
    categorywise.filter(col("category")==="High Readmission Risk").groupBy("category").agg(avg(col("readmission_interval"))).show()
    categorywise.filter(col("category")==="Moderate Risk").filter(col("admission_type")==="ICU" && col("icu_admissions")>2).show()
  }
}