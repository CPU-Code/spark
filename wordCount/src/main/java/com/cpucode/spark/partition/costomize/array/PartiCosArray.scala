package com.cpucode.spark.partition.costomize.array

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author CpuCode
 * @Github https://github.com/CPU-Code
 * @YuQue https://www.yuque.com/cpucode
 * @Date 2022/8/24 16:45
 */
object PartiCosArray {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("PartiCosArray")
    val sc = new SparkContext(conf)
    //1 4个数据，设置4个分区，输出：0分区->1，1分区->2，2分区->3，3分区->4
    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 4)

    //2）4个数据，设置3个分区，输出：0分区->1，1分区->2，2分区->3,4
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4), 3)

    //3）5个数据，设置3个分区，输出：0分区->1，1分区->2、3，2分区->4、5
    val rdd2 = sc.makeRDD(Array(1, 2, 3, 4, 5), 3)

    //输出数据

    rdd.collect.foreach(println)
    println("/*********************************/")
    rdd1.collect.foreach(println)
    println("/*********************************/")
    rdd2.collect.foreach(println)
//    rdd.saveAsTextFile("wordCount/output/rdd")
//    rdd1.saveAsTextFile("wordCount/output/rdd1")
//    rdd2.saveAsTextFile("wordCount/output/rdd2")

    //关闭连接
    sc.stop()
  }
}
