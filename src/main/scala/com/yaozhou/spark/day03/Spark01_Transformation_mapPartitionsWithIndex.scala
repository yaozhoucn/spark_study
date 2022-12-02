package com.yaozhou.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/11/29 12:20
 * @Desc:
 */
object Spark01_Transformation_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark07_Transformation_mapPartitions").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    /**
     * 以分区为单位，对RDD中的元素进行映射
     * 一般适合用于批处理的操作，比如：将RDD的中的元素插入到数据库中，需要连接数据库；
     * 如果每一个元素都创建一个的连接，效率很低；可以对每个分区的元素创建一个连接；
     */
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
