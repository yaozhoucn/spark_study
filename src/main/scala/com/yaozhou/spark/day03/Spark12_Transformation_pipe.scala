package com.yaozhou.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *    pipe
 *
 *    管道，针对每个分区，都调用一次shell脚本，返回输出的RDD
 *
 */
object Spark12_Transformation_pipe {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark12_Transformation_pipe").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List("hi", "Hello", "how", "are", "you"), 1)


    //使用pipe调用shell脚本
    rdd.pipe("/opt/module/spark/pipe.sh").collect()

    sc.stop()
  }

}
