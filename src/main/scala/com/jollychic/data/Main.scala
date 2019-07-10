package com.jollychic.data

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    // 设置为本地模式2核，应用名为Count
    val conf = new SparkConf().setAppName("Count").setMaster("local[2]")
    val sc = new SparkContext(conf)

    /*
    /** 统计单词个数 */
    val input = sc.parallelize(List("hello world", "hello scala"))

    // 根据" "将单词分割
    val words = input.flatMap(x => x.split(" "))

    // 转化为map：每个单词作为key，其值为1，并调用reduceByKey将相同key的value相加
    val result = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    // 输出结果 (scala,1),(hello,2),(world,1)
    println(result.collect().mkString(","))

    val result2 = words.countByValue()
    println(result2.mkString(","))

    val data = Seq(("a",3),("b",4),("a",1))
    // 自定义并行度=10
    sc.parallelize(data).reduceByKey((x, y) => x + y, 2).collect.foreach(println)


    val list = List("hadoop","spark","hive","spark")
    val rdd = sc.parallelize(list)
    val pairRdd = rdd.map(x => (x,1))
    pairRdd.groupByKey().collect.foreach(println)*/

    sc.textFile("C:\\Users\\WIN7\\Desktop\\测试数据\\标签、智能定价消费者.txt")


    sc.stop()
  }
}