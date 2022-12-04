package com.yaozhou.spark.day02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Author: HANG
 * @Date: 2022/11/29 12:20
 * @Desc:
 */
object Spark06_Transformation_map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark06_Transformation_map").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3),2)
    println("原分区数："+rdd.partitions.size)
    val newRdd: RDD[Int] = rdd.map(_ * 2)
    println("=====================================================")
    println("新分区数："+newRdd.partitions.size)
    newRdd.collect().foreach(println)

    sc.stop()
  }

}
