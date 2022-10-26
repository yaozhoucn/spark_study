package com.yaozhou.spark.day02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Author: HANG
 * @Date: 2022/10/26 10:58
 * @Desc:
 *
 *  -从集合中创建RDD
 *    默认分区规则 --取决于分配给应用的CPU核数
 *  -指定分区数
 *    math.min(取决于应用分配给应用的CPU核数，2)
 */
object Spark04_Partition_Mem {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark04_Partition_Mem").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 5)

    //通过集合创建RDD
    //默认分区数，集合中4给数据，实际输出数据20个分区数，分区中数据分布情况
    val rdd: RDD[Int] = sc.makeRDD(list,3)

    //通过读取外部文件数据查看分区效果
    val rdd2: RDD[String] = sc.textFile("D:\\JavaProjects\\IntelliJIDEAWorkspace\\wordcount_spark\\input")
    //查看分区效果
    println(rdd.partitions.size)

    rdd.saveAsTextFile("D:\\JavaProjects\\IntelliJIDEAWorkspace\\wordcount_spark\\output")
  }

}
