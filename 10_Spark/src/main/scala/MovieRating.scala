import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object MovieRatings{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Movie Ratings Application")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR");

    val userRatingsRDD = sc.textFile("hdfs://localhost:9000/spark_data/u.data")
    val userRatingsTupleRDD = userRatingsRDD.map(line => (line.split("\t")(1), line.split("\t")(2).toFloat))
    //userRatingsTupleRDD.foreach(println)
    val groupedUserRatingsTupleRDD = userRatingsTupleRDD.groupByKey().mapValues(_.toList)
    //groupedUserRatingsTupleRDD.foreach(println)
    
    def average[T](ts:List[T])( implicit num:Numeric[T]) = {
       num.toDouble(ts.sum) / ts.size
    }
    val averageRatingByMovieId = groupedUserRatingsTupleRDD.map(rec => (rec._1, average(rec._2)))
    //averageRatingByMovieId.foreach(println)

    val userItemRDD = sc.textFile("hdfs://localhost:9000/spark_data/u.item")
    val userItemTupleRDD = userItemRDD.map(line => (line.split("\\|")(0), (line.split("\\|")(0), line.split("\\|")(1), line.split("\\|")(2), line.split("\\|")(4))))
    //userItemTupleRDD.foreach(println)

    val joinedRDD = userItemTupleRDD.join(averageRatingByMovieId)
    //joinedRDD.foreach(println)

    val finalResultRDD = joinedRDD.map(rec => rec._2._1._1 + "," + rec._2._1._2 + "," + rec._2._1._3+","+rec._2._1._4 + "," + rec._2._2)
    //finalResultRDD.foreach(println)
    finalResultRDD.saveAsTextFile("hdfs://localhost:9000/spark_data_out")
    
    sc.stop()
  }
}

