import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object problem2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("problem2").master("local[*]").getOrCreate()
    import spark.implicits._
    val df = spark.read.format("csv").option("header", true).option("path", "C:/Users/Admin/Downloads/problem2.csv").load()
    val df1 = df.withColumn("rating_category",
      when(col("rating")>=8,"Excellent").when(col("rating")>8 && col("rating")>=6,"Good").otherwise("Average"))
    df1.show()


    val df2 = df1.withColumn("duration_category",
      when(col("duration_minutes")>150,"Long").when(col("duration_minutes")>90 && col("duration_minutes")>=150,"Medium").otherwise("short"))
    df2.filter(col("movie_name").startsWith("T")).show()


  }
}
