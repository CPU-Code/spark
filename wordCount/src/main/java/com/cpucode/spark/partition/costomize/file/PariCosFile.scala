package com.cpucode.spark.partition.constomize.file

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author CpuCode
 * @Github https://github.com/CPU-Code
 * @YuQue https://www.yuque.com/cpucode
 * @Date 2022/8/25 18:07
 */
object PariCosFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PariCosFile").setMaster("local[*]")

    val sc = new SparkContext(conf)

    // 每行一个数字
    val rdd = sc.textFile("wordCount/src/main/resources/word.txt", 3)

    rdd.saveAsTextFile("wordCount/output")

    sc.stop()
  }
}
