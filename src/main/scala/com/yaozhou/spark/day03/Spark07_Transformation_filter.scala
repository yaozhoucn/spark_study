package com.yaozhou.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/11/29 12:20
 * @Desc:
 */
object Spark07_Transformation_filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark07_Transformation_filter").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("xiaolin", "xiaopeng", "xiaoqiang", "ceshi", "haohao"))

    //1.取出Rdd中包含xiao的字符串

    val filterRdd: RDD[String] = rdd.filter(_.contains("xiao"))

    filterRdd.collect().foreach(println)

    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9))

    //2.取出数据中的奇数

    val filterRdd2: RDD[Int] = rdd2.filter(_ % 2 != 0)

    filterRdd2.collect().foreach(println)

    sc.stop()
  }

}
