package com.yaozhou.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/11/29 12:20
 * @Desc:
 */
object Spark06_Transformation_groupBy_WordCount2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark05_Transformation_groupBy_WordCount").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    /**
     * 使用groupBy实现wordcount
     */

    //复杂版-实现方式1

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Hello Scala", 2), ("Hello Spark", 3), ("Hello World", 2)))
    //1.对Rdd中的数据进行扁平映射

//    val mapRdd: RDD[(String)] = rdd.map{
//      case (word,count) => {
//        (word +" ") * count
//      }
//    }
//    val flatRDD: RDD[String] = mapRdd.flatMap(_.split(" "))
//
//    val groupRDD: RDD[(String, Iterable[String])] = flatRDD.groupBy(elem => elem)
//
//    val wordCount: RDD[(String, Int)] = groupRDD.map {
//      case (word, list) => {
//        (word, list.size)
//      }
//    }



    //复杂版-实现方式2
    //对Rdd中的元素进行扁平映射
    val flapRdd: RDD[(String, Int)] = rdd.flatMap {
      case (words, count) => {
        words.split(" ").map(word => (word, count))
      }
    }

    val groupRdd: RDD[(String, Iterable[(String, Int)])] = flapRdd.groupBy(elem => elem._1)
    val wordCount: RDD[(String, Int)] = groupRdd.map {
      case (word, datas) => {
        (word, datas.map(_._2).sum)
      }
    }

    wordCount.collect().foreach(println)
    sc.stop()
  }

}
