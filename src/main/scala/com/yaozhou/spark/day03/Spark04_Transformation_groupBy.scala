package com.yaozhou.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/11/29 12:20
 * @Desc:
 */
object Spark04_Transformation_groupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark04_Transformation_groupBy").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    /**
     * 按照Rdd指定的规则对数据进行分组
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6,7,8,9), 3)
    val newRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(datas => datas % 2)

    newRDD.mapPartitionsWithIndex((index,datas)=>{
      datas.map((index,_))
    }).collect().foreach(println)


    sc.stop()
  }

}
