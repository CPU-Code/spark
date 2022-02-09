package com.cpucode.spark.wc.word.count

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : cpucode
 * @date : 2022/2/9 18:14
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
object WordCountEnv {
  def main(args: Array[String]): Unit = {
    // Spark是一个计算【框架】
    // 1. 能找到他 ：增加依赖
    // 2. 获取Spark的连接（环境）
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val context = new SparkContext(conf)

    // 读取文件
    val lines = context.textFile("wordCount/src/main/resources/word.txt")

    // 将文件中的数据进行了分词
    val values = lines.flatMap(_.split(" "))

    // 将分词后的数据进行了分组
    val value = values.groupBy(word => word)

    // 对分组后的数据进行统计分析
    val wordCount = value.mapValues(_.size)

    // 将统计结果打印在控制台上
    wordCount.collect().foreach(println)

    context.stop()
  }
}
