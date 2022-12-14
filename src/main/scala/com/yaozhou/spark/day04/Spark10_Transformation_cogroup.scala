package com.yaozhou.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *
 *  jcogroup 类似全连接，但是在同一个RDD中对key聚合
 *
 */
object Spark10_Transformation_cogroup{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark10_Transformation_cogroup").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    //创建第一个Rdd
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))

    //创建第二个Rdd
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6),(2,8)))


    //cogroup先把同集合相同Key的数据进行聚合，再把不同Rdd中相同key的数据进行聚合


    val cogroupRdd: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd.cogroup(rdd2)

    cogroupRdd.collect().foreach(println)




    sc.stop()
  }


}

