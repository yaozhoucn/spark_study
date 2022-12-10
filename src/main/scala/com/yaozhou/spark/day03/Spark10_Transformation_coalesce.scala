package com.yaozhou.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *    重新分区的算子 coalesce，repartition
 *
 *    coalesce -- 默认不执行shuffle，一般用于缩减分区
 *
 *    repartition  -- 底层调用的就是coalesce，只不过默认是执行shuffle的，一般用于扩大分区
 *
 */
object Spark10_Transformation_coalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark10_Transformation_coalesce").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 4)
    println("===============缩减分区之前====================")
    rdd.mapPartitionsWithIndex((index,datas) => {
      println(index+"------>"+datas.mkString(","))
      datas
    }).collect().foreach(println)

    //缩减分区

    println("===============缩减分区之后====================")

    //注意:默认情况下，如果使用coalesce扩大分区是不起作用的，因为没有进行shuffle
    rdd.coalesce(3).mapPartitionsWithIndex(
      (index,datas) => {
        println(index+"------>"+datas.mkString(","))
        datas
      }
    ).collect().foreach(println)

    //增加分区
    println("===============增加分区之后====================")
    rdd.repartition(6).mapPartitionsWithIndex(
      (index,datas) => {
        println(index+"------>"+datas.mkString(","))
        datas
      }
    ).collect().foreach(println)
    sc.stop()
  }

}
