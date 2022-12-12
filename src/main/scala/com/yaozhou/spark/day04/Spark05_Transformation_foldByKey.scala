package com.yaozhou.spark.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *
 *  foldByKey 分区内和分区间相同的aggregateByKey()
 *
 *  参数zeroValue:是-个初始化值，它可以是任意类型
 *  参数func:是一个函数，两个输入参数相同
 *
 */
object Spark05_Transformation_foldByKey{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark05_Transformation_foldByKey").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    //创建kv形式的
    val rdd = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)),2)
/*    println("====================重新分区之前=====================")

    rdd.mapPartitionsWithIndex{
      (index,datas) => {
        println(index+"=========>"+datas.mkString(","))
        datas
      }
    }.collect().foreach(println)*/

    //如果分区内和分区间的计算呢规则一样，并且不需要指定初始值，那么优先使用reduceByKey
    println("===============使用reduceByKey======================")
    rdd.reduceByKey(_+_).collect().foreach(println)


    //如果分区内和分区间的计算规则一样，并且需要初始值，那么优先考虑使用foldByKey
    println("===============foldByKey======================")
    rdd.foldByKey(0)(_+_).collect().foreach(println)


    //如果分区内与分区间的计算规则不一样，并且需要初始值，那么优先考虑使用aggregateByKey
    println("===============aggregateByKey======================")
    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)




    sc.stop()
  }


}

