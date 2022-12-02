package com.yaozhou.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/11/29 12:20
 * @Desc:
 */
object Spark07_Transformation_mapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark07_Transformation_mapPartitions").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),4)
    println("原分区数："+rdd.partitions.size)
    val newRdd: RDD[Int] = rdd.mapPartitions((datas) => {
      datas.map(x =>
        x * 2
      )
    })

    /**
     * 简化写法
     *
     *
     * rdd.mapPartitions(datas => datas.map(_*2))
     *
     */
    println("=====================================================")
    println("新分区数："+newRdd.partitions.size)
    newRdd.collect().foreach(println)

    sc.stop()
  }

}
