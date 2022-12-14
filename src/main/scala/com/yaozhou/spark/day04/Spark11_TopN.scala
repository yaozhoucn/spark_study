package com.yaozhou.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *
 *  需求：统计每个省份的广告点击Top3
 *
 */
object Spark11_TopN{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark10_Transformation_cogroup").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    //1.读取文件创建Rdd
    val textRdd: RDD[String] = sc.textFile("D:\\JavaProjects\\IntelliJIDEAWorkspace\\wordcount_spark\\input\\agent.log")

    //2.对读取的数据进行结构映射（省份id-广告id，1）

    val mapRdd: RDD[(String, Int)] = textRdd.map {
      case line => {

        //2.1 用空格对读取到的数据进行分割
        val datas: Array[String] = line.split(" ")

        //2.2 封装为元组数据进行返回
        (datas(1) + "-" + datas(4), 1)
      }
    }
    //3.数据结果普转换省份进行分组，并计算每一个点击广告的聚合

    val reduceRdd: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)


    //4.再次对数据进行结构转换映射（省份i，（广告id，次数））

    val mapRdd2: RDD[(String, (String, Int))] = reduceRdd.map {
      case (str, i) => {
        val datas: Array[String] = str.split("-")
        (datas(0), (datas(1), i))
      }
    }



   //5.根据省份进行分组
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd2.groupByKey()




    //6.对每一个省份的广告点击次数进行降序排序
    val sortRdd: RDD[(String, List[(String, Int)])] = groupRdd.mapValues {
      datas => {
        datas.toList.sortWith(
          (a, b) => {
            a._2 > b._2
          }
        ).take(3)
      }
    }

    sortRdd.collect().foreach(println)





    //关闭连接

    sc.stop()
  }


}

