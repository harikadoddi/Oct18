//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{avg, col, count, current_date, date_sub, initcap, sum, when}
//
//object code14 {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName("code14").master("local[*]").getOrCreate()
//    import spark.implicits._
//
//    val loanApplicants = List(
//      ("karthik", 60000, 120000, 590),
//      ("neha", 90000, 180000, 610),
//      ("priya", 50000, 75000, 680),
//      ("mohan", 120000, 240000, 560),
//      ("ajay", 45000, 60000, 620),
//      ("vijay", 100000, 100000, 700),
//      ("veer", 30000, 90000, 580),
//      ("aatish", 85000, 85000, 710),
//      ("animesh", 50000, 100000, 650),
//      ("nishad", 75000, 200000, 540)
//    ).toDF("name", "income", "loan_amount", "credit_score")
//
//    /*1. Classify loan applicants as "High Risk" if the loan amount exceeds twice their income and
//credit score is below 600, "Moderate Risk" if the loan amount is between 1-2 times their
//income and credit score between 600-700, and "Low Risk" otherwise. Find the total count of
//each risk level.
//2. For applicants classified as "High Risk," calculate the average loan amount by income range
//(e.g., < 50k, 50-100k, >100k).
//3. Group by income brackets (<50k, 50-100k, >100k) and calculate the average credit score for
//each risk level. Filter for groups where average credit score is below 6508
//     */
//
//    val df1= loanApplicants.withColumn("risklevel",
//      when(col("loan_amount")> col("income")*2 && col("credit_score")<600, "High Risk").
//        when(col("loan_amount").between(col("income"),col("income")*2) && col("credit_score").between(600,700),"Moderate_risk").
//        otherwise("low_risk"))
//      df1.withColumn((col("category").equals("High Risk"), df1.groupBy("category").agg(avg("category")))).show()
//
//
//
//
//  }
//
//}
//
