package com.yaozhou.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *
 *
 *   union()并集
 *
 *   subtract ()差集
 *
 *   intersection()交集
 *
 *   zip()拉链
 *
 */
object Spark13_Transformation_doubleValue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark13_Transformation_doubleValue").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    //创建两个Rdd
    val rdd1 = sc.makeRDD(List(1, 2, 4, 6))
    val rdd2 = sc.makeRDD(List(2, 5, 6, 8))

    //union()并集
    rdd1.union(rdd2).collect().foreach(println)

    //subtract ()差集
    rdd1.subtract(rdd2).collect().foreach(println)

    //intersection()交集
    rdd1.intersection(rdd2).collect().foreach(println)

    //zip()拉链  分别取一个元素组成元组

    /**
     * (1,2)
        (2,5)
        (4,6)
        (6,8)
     */
    rdd1.zip(rdd2).collect().foreach(println)




    sc.stop()
  }

}
