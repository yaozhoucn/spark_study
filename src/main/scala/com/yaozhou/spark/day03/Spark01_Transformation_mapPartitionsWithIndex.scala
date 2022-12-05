package com.yaozhou.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/11/29 12:20
 * @Desc:
 */
object Spark01_Transformation_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark01_Transformation_mapPartitionsWithIndex").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    /**
     * 以分区为单位，对RDD中的元素进行映射
     * 一般适合用于批处理的操作，比如：将RDD的中的元素插入到数据库中，需要连接数据库；
     * 如果每一个元素都创建一个的连接，效率很低；可以对每个分区的元素创建一个连接；
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    println("原分区数："+rdd.partitions.size)
//    val newRdd: RDD[(Int,Int)] = rdd.mapPartitionsWithIndex((index,datas) => {
//      datas.map((index,_))
//    })

    //需求：第二个分区数据*2，其余分区数据保持不变；

    val newRdd: RDD[Int] = rdd.mapPartitionsWithIndex((index,datas) => {
//      index match {
//        case 1 => datas.map(_*2)
//        case _ => datas
//      }
      if (index == 1){
        datas.map(_*2)
      }else{
        datas
      }
    })
    println("=====================================================")
    println("新分区数："+newRdd.partitions.size)
    newRdd.collect().foreach(println)

    sc.stop()
  }

}
