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
    val conf = new SparkConf().setAppName("Spark05_Partition_File").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3))
    val newRdd: RDD[Int] = rdd.map(_ * 2)
    newRdd.collect().foreach(println)

    sc.stop()
  }

}
