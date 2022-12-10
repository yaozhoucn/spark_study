package com.yaozhou.spark.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *
 *  reduceBy 根据key对value进行聚合
 *
 *
 */
object Spark03_Transformation_groupByKey{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark03_Transformation_groupByKey").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    //创建kv形式的
    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)),3)
/*    println("====================重新分区之前=====================")

    rdd.mapPartitionsWithIndex{
      (index,datas) => {
        println(index+"=========>"+datas.mkString(","))
        datas
      }
    }.collect().foreach(println)*/

    //根据key对value进行分组
    val groupByKeyRdd = rdd.groupByKey()

    //进行word count
    groupByKeyRdd.map{
      case (word,datas) => {
        (word,datas.sum)
      }
    }.collect().foreach(println)


    sc.stop()
  }


}

