package com.cpucode.spark.create.rdd

import org.apache.spark.{SparkConf, SparkContext}

object CreateRddArray {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("CreateRddArray").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val context = new SparkContext(conf)

    //3.使用parallelize()创建rdd
    val rdd = context.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8))
    rdd.collect().foreach(println)

    println("/******************************************************/")
    //4.使用makeRDD()创建rdd
    val rdd1 = context.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8))
    rdd1.collect().foreach(println)

    context.stop()
  }
}
