package com.cpucode.spark.map.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author CpuCode
 * @Github https://github.com/CPU-Code
 * @YuQue https://www.yuque.com/cpucode
 * @Date 2022/8/25 18:46
 */
object MapValue {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("MapValue")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3具体业务逻辑
    // 3.1 创建一个RDD
    val rdd = sc.makeRDD(1 to 4, 2)

    // 3.2 调用map方法，每个元素乘以2
    val mapRDD = rdd.map(_ * 2)

    // 3.3 打印修改后的RDD中数据
    mapRDD.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}