import org.apache.spark.SparkContext

import java.io.{File, PrintWriter}

object hello{
  def main(args: Array[String]): Unit = {
      val sc = new SparkContext("local[*]","harika")
      val data = sc.textFile("C:/Users/Admin/OneDrive/Desktop/info.txt")
      val file = new File("C:/Users/Admin/OneDrive/Desktop/modified.txt")
      val data1 = data.map(x => x.toInt)

      val printwriter = new PrintWriter(file)
      val result = data1.map{x => if(x%2==0){ x+"-"+x.toString.length+"-even"}
        else{ (x+"-"+x.toString.length+"-odd")}}.collect()

      result.foreach(line => printwriter.write(line+ "\n"))

      printwriter.close()
  }
}
