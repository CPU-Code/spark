package com.cpucode.spark.wc.reduce

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : cpucode
 * @date : 2022/2/9 19:29
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
object WcReduce {
  def main(args: Array[String]): Unit = {
    // Spark是一个计算【框架】
    // 1. 能找到他 ：增加依赖
    // 2. 获取Spark的连接（环境）
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")

    val context = new SparkContext(conf)
    // 读取文件
    val lines = context.textFile("wordCount/src/main/resources/word.txt")

    // 将文件中的数据进行了分词
    // word => (word, 1)
    val values = lines.flatMap(_.split(" "))
    val function = values.map((_, 1))

    // 将分词后的数据进行了分组
    val value = function.groupBy(_._1)

    // word => List((word,1), (word,1)) => List((word, 2))
    // 对分组后的数据进行统计分析
    val wordCount = value.mapValues(
      list => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )._2
      }
    )

    // 将统计结果打印在控制台上
    wordCount.collect().foreach(println)

    context.stop()
  }
}
