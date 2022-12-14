package com.yaozhou.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *
 *  join()连接 将相同key对应的多个value关联在一起
 *
 */
object Spark09_Transformation_join{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark09_Transformation_join").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    //创建第一个Rdd
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))

    //创建第二个Rdd
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6),(2,8)))


    //join相当于连接，将两个RDD中key相同的数据匹配，如果key匹配不上，那么数据不显示
    rdd.join(rdd2).collect().foreach(println)
    println("===================================")
    //rdd2.join(rdd).collect().foreach(println)


    rdd.leftOuterJoin(rdd2).collect().foreach(println)




    sc.stop()
  }


}

