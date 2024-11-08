import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, when}

object dataframes {
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName("harika").master("local[*]").getOrCreate()
//    val df = spark.read.format("csv").option("header",false).option("inferschema",true).option("path","C:/Users/Admin/OneDrive/Documents/Book1.csv").load()
//    df.printSchema()
//    df.show()

//    val sparkconf= new SparkConf()
//    sparkconf.set("spark.app.name","spark-program")
//    sparkconf.set("spark.master","local[*]")
//
//    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
//
//    val schema = StructType(List(
//      StructField("id",IntegerType),
//      StructField("emp_name",StringType),
//      StructField("manager_name",StringType),
//      StructField("salary",IntegerType)
//
//    ))
//
//    val df = spark.read.format("csv").option("header",true).schema(schema).option("path","C:/Users/Admin/OneDrive/Documents/Book1.csv").load()
//
//    df.select(
//      col("id"),
//      col("salary"),
//      when(col("salary")>100,"rich").otherwise("poor").alias("status")
//    ).show()
//
//    df.withColumn("status",
//      when(col("salary")>=10,"rich").otherwise("poor").alias("status")).show()
//    df.printSchema()
//    df.show()

    val spark = SparkSession.builder().appName("spark-program").master("local[*]").getOrCreate()
  import spark.implicits._
    val list = List((1,"harika",24),(2,"harish",25),(3,"chinni",26)).toDF("id","name","age")

//    list.select(col("id"),col("name"),when(col("age")>5,"big").otherwise("smal").alias("elder/order")).show()

    list.select(col("id"),col("name").startsWith("h")).show()
    list.filter(col("age")>20 && col("name").startsWith("h")).show()





  }

}
