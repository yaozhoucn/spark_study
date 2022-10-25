package com.yaozhou.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/10/25 16:17
 * @Desc: 通过读取内存当中的集合，创建RDD
 */
object Spark01_CreateRDD_Mem {
  def main(args: Array[String]): Unit = {
    //创建spark配置文件
    val conf = new SparkConf().setAppName("Spark01_CreateRDD_Mem").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    //创建一个集合对象
    val list = List(1, 2, 3, 4)
    //根据集合创建RDD 方式1
    val rdd: RDD[Int] = sc.parallelize(list)


    // //根据集合创建RDD 方式2 底层调用 parallelize(seq, numSlices)
    val rdd2: RDD[Int] = sc.makeRDD(list)

    rdd2.collect().foreach(println)

    //关闭连接
    sc.stop()
  }

}
