import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, max, min, sum}
object problem7 {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().appName("problem6").master("local[*]").getOrCreate()
    import spark.implicits._
//    val purchaseData = Seq(
//      ("Customer1", "Product1", 100),
//      ("Customer1", "Product2", 150),
//      ("Customer1", "Product3", 200),
//      ("Customer2", "Product2", 120),
//      ("Customer2", "Product3", 180),
//      ("Customer3", "Product1", 80),
//      ("Customer3", "Product3", 250)
//
//    ).toDF("Customer", "Product","Amount")

//    purchaseData.groupBy("customer").agg(countDistinct(col("product")),sum(col("amount"))).show()

    val ratingData = List(
      ("User1", "Movie1", "Action", 4.5),
      ("User1", "Movie2", "Drama", 3.5),
      ("User1", "Movie3", "Comedy", 2.5),
      ("User2", "Movie1", "Action", 3.0),
      ("User2", "Movie2", "Drama", 4.0),
      ("User2", "Movie3", "Comedy", 5.0),
      ("User3", "Movie1", "Action", 5.0),
      ("User3", "Movie2", "Drama", 4.5),
      ("User3", "Movie3", "Comedy", 3.0)
    ).toDF("User", "Movie", "Genre", "Rating")

    ratingData.groupBy("User","genre").agg(avg(col("rating"))).show()
  }

}
