package com.yaozhou.spark.day04

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *
 *  reduceBy 根据key对value进行聚合
 *
 *
 */
object Spark02_Transformation_reduceByKey{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark02_Transformation_reduceByKey").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    //创建kv形式的
    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)),3)

    //根据key对value进行聚合
    rdd.reduceByKey(_+_).collect().foreach(println)


    sc.stop()
  }

}

