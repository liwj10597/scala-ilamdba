import org.apache.spark.{SparkContext,SparkConf}

object Main {
  def main(args: Array[String]): Unit = {
    // 设置为本地模式2核，应用名为Count
    val conf = new SparkConf().setAppName("Count").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val input = sc.parallelize(List("hello world", "hello scala"))
    // 根据" "将单词分割
    val words = input.flatMap(x => x.split(" "))
    // 转化为map：每个单词作为key，其值为1，并调用reduceByKey将相同key的value相加
    val result = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    // 输出结果 (scala,1),(hello,2),(world,1)
    println(result.collect().mkString(","))


    sc.stop()
  }
}