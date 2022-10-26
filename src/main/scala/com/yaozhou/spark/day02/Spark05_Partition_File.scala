package com.yaozhou.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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
object Spark05_Partition_File {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark05_Partition_File").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)



    //通过读取外部文件数据查看分区效果
    val rdd: RDD[String] = sc.textFile("D:\\JavaProjects\\IntelliJIDEAWorkspace\\wordcount_spark\\input\\2.txt",3)
    //查看分区效果

    rdd.saveAsTextFile("D:\\JavaProjects\\IntelliJIDEAWorkspace\\wordcount_spark\\output")
    println(rdd.partitions.size)
    rdd.collect().foreach(println)
  }

}
