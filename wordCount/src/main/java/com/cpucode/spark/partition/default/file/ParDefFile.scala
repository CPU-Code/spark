package com.cpucode.spark.partition.default.file

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author CpuCode
 * @Github https://github.com/CPU-Code
 * @YuQue https://www.yuque.com/cpucode
 * @Date 2022/8/24 17:27
 */
object ParDefFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ParDefFile")
    val sc = new SparkContext(conf)

    //默认分区的数量：默认取值为当前核数和2的最小值
    val rdd = sc.textFile("wordCount/src/main/resources/word.txt")

    rdd.saveAsTextFile("wordCount/output")

    sc.stop()
  }
}
