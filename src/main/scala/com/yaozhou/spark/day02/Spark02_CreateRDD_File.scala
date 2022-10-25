package com.yaozhou.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/10/25 17:18
 * @Desc: 从外出存储系统中读取数据创建RDD
 */
object Spark02_CreateRDD_File {
  def main(args: Array[String]): Unit = {
    //创建spark配置文件
    val conf = new SparkConf().setAppName("Spark02_CreateRDD_File").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    //从本地文件中读取文件数据，创建RDD
    val rdd: RDD[String] = sc.textFile("D:\\JavaProjects\\IntelliJIDEAWorkspace\\wordcount_spark\\input\\1.txt")


    //从HDFS服务上读取数据创建RDD
    val rdd2: RDD[String] = sc.textFile("hdfs://hadoop202:8020/inoput")
    rdd.collect().foreach(println)
  }

}
