package com.yaozhou.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/11/29 12:20
 * @Desc:
 */
object Spark09_Transformation_distinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark08_Transformation_sample").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 4, 5, 7, 32, 1, 2, 4),5)

    /**
     * withScope {
    map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
    (1,null)  ==>    (1,(null,null,null))  ==> (1,null)
    (1,null)
    (1,null)
                }
     */
    rdd.distinct().collect().foreach(println)

    //对rdd进行数据结构转换映射

    val nullRdd: RDD[(Int, Null)] = rdd.map(x => (x, null))
    val reduceByKeyRdd: RDD[(Int, Null)] = nullRdd.reduceByKey((x, y) => x)

    println("=============================")

    reduceByKeyRdd.collect().foreach(println)

    println("=============================")

    reduceByKeyRdd.map(_._1).collect().foreach(println)




    sc.stop()
  }

}
