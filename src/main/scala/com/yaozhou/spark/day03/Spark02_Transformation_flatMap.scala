package com.yaozhou.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/11/29 12:20
 * @Desc:
 */
object Spark02_Transformation_flatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark02_Transformation_flatMap").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    /**
     * 以分区为单位，对RDD中的元素进行映射
     * 一般适合用于批处理的操作，比如：将RDD的中的元素插入到数据库中，需要连接数据库；
     * 如果每一个元素都创建一个的连接，效率很低；可以对每个分区的元素创建一个连接；
     */
    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1,2,3),List(4,5,6),List(7,8),List(9)),2)
    val newRDD: RDD[Int] = rdd.flatMap(datas => datas)



    newRDD.collect().foreach(println)
    //关闭连接
    sc.stop()
  }

}
