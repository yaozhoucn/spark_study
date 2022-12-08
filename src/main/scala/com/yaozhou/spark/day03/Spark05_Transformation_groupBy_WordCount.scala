package com.yaozhou.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/11/29 12:20
 * @Desc:
 */
object Spark05_Transformation_groupBy_WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark05_Transformation_groupBy_WordCount").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    /**
     * 使用groupBy实现wordcount
     */

    //简单版-实现方式1

    val rdd: RDD[String] = sc.makeRDD(List("Hello world", "Hello scala", "Hello spark"))
    //1.对Rdd中的数据进行扁平映射
    val wordRdd: RDD[String] = rdd.flatMap(datas => datas.split(" "))

    //2.将映射后的数据进行结构转换，为每个单词进行计数
    val wordToOneRdd: RDD[(String, Int)] = wordRdd.map(elem => (elem, 1))

    //3.按照Key对Rdd中的元素进行分组
    val groupByRdd: RDD[(String, Iterable[(String, Int)])] = wordToOneRdd.groupBy(elem => elem._1)

    //将groupBY获得的数据进行格式转化，将分组后的数据再进行映射
    //val wordCount: RDD[(String, Int)] = groupByRdd.map(elem => (elem._1, elem._2.size))
    val wordCount: RDD[(String, Int)] = groupByRdd.map {
      case (word, data) => {
        (word, data.size)
      }
    }
    wordCount.collect().foreach(println)
    sc.stop()
  }

}
