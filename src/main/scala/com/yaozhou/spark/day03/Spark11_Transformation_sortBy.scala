package com.yaozhou.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *    sortBy
 *

 *
 */
object Spark11_Transformation_sortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark11_Transformation_sortBy").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 4)
    //默认升序排序
    rdd.sortBy(num => num).collect().foreach(println)

    //降序排序
    rdd.sortBy(num => num,false).collect().foreach(println)


    val rdd2 = sc.makeRDD(List("1", "2", "43", "5", "0"))

    //按照字符串字符字典顺序进行排序

    rdd2.sortBy(elem => elem).collect().foreach(println)

    //按照将字符串转换为整数后进行排序

    rdd2.sortBy(_.toInt).collect().foreach(println)

    sc.stop()
  }

}
