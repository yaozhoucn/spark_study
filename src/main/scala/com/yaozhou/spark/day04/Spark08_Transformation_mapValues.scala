package com.yaozhou.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *
 *  针对kv形式的类型，只对value进行操作
 *
 */
object Spark08_Transformation_mapValues{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark08_Transformation_mapValues").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    //创建kv形式的
    val rdd = sc.makeRDD(List(("xiaowang", 45), ("xiaoli", 87), ("xiaowang", 78), ("xiaoli", 3), ("xiaowang", 98), ("xiaoli", 99)),2)

    //创建一份pairRDD，并将value添加字符串“|||”

    rdd.mapValues(_.toString+"|||").collect().foreach(println)
    sc.stop()
  }


}

