package com.cpucode.spark.wc.reduceByKey

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : cpucode
 * @date : 2022/2/9 19:52
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
object ReduceByKey {
  def main(args: Array[String]): Unit = {

    // Spark是一个计算【框架】
    // 1. 能找到他 ：增加依赖
    // 2. 获取Spark的连接（环境）
    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val context = new SparkContext(conf)

    // 读取文件
    val lines = context.textFile("wordCount/src/main/resources/word.txt")

    // 将文件中的数据进行了分词
    val word = lines.flatMap(_.split(" "))
    // word => (word, 1)
    val wordToOne = word.map((_, 1))

    // reduceByKey : 按照key分组， 对相同的key的value进行reduce
    // (word, 1)(word, 1)(word, 1)(word, 1)(word, 1)
    // reduce(1,1,1,1,1)
    // 框架的核心就是封装
    val wordCount = wordToOne.reduceByKey(_ + _)

    // 将统计结果打印在控制台上
    wordCount.collect().foreach(println)

    context.stop()
  }
}
