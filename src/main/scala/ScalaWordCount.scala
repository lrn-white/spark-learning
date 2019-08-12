import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("ScalaWordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/home/lrn/IdeaProjects/spark-learning/src/main/scala/words.txt")
    val pairwords= lines.map(word =>Tuple2(word,1))
    val reduce = pairwords.reduceByKey((v1:Int,v2:Int)=>v1+v2)
    reduce.foreach(println)
    sc.stop()
  }
}