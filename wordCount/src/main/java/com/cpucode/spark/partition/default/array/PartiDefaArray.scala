package com.cpucode.spark.partition.default.array

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author CpuCode
 * @Github https://github.com/CPU-Code
 * @YuQue https://www.yuque.com/cpucode
 * @Date 2022/8/24 16:00
 */
object PartiDefaArray {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("PartiDefaArray").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(1, 2, 3, 4))
    //3. 输出数据，产生了 12 个分区
    rdd.saveAsTextFile("wordCount/output")

    //4.关闭连接
    sc.stop()
  }
}
