package com.yaozhou.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/11/29 12:20
 * @Desc:
 */
object Spark03_Transformation_glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark03_Transformation_glom").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    /**
     * glom将Rdd一个分区中的元素，组合成一个新的数组
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    print("----------没有glom之前-------------")
    rdd.mapPartitionsWithIndex((index, datas) => {
      println(index+"--------->" + datas.mkString(","))
      datas
    }).collect().foreach(print)


    println("----------有glom之后-------------")
    val newRDD: RDD[Array[Int]] = rdd.glom()

    newRDD.mapPartitionsWithIndex((index, datas) => {
      println(index+"--------->" + datas.next().mkString(","))
      datas
    }).collect().foreach(println)

    //获取最大值
    val maxRdd: RDD[Int] = newRDD.map(_.max)

//    for (elem <- maxRdd.collect()) {
//      println("集合中的最大值分别为" + elem)
//    }
    maxRdd.collect().foreach(elem => println("集合中的最大值分别为" + elem))
    println("集合的最大值的和为"+maxRdd.collect().sum)

    //newRDD.collect().foreach(println)
    //关闭连接

    sc.stop()
  }

}
