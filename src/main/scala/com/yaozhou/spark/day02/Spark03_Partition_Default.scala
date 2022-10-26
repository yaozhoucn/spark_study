package com.yaozhou.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/10/25 17:29
 * @Desc: 默认分区
 *
 *  -从集合中创建RDD 分区大小取决于分配给应用的CPU核数
 *  -读取外部文件创建RDD 分区大小取决于分配给应用的CPU核数与2取一个最小值 --math.min(取决于应用分配给应用的CPU核数，2)
 */
object Spark03_Partition_Default {
  def main(args: Array[String]): Unit = {
    //创建spark配置文件
    val conf = new SparkConf().setAppName("Spark02_CreateRDD_File").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    val list = List(1, 2, 3, 5)

    //通过集合创建RDD

    val rdd: RDD[Int] = sc.makeRDD(list)

    //通过读取外部文件数据查看分区效果
    val rdd2: RDD[String] = sc.textFile("D:\\JavaProjects\\IntelliJIDEAWorkspace\\wordcount_spark\\input")
    //查看分区效果
    println(rdd2.partitions.size)

    rdd2.saveAsTextFile("D:\\JavaProjects\\IntelliJIDEAWorkspace\\wordcount_spark\\output")
  }

}
