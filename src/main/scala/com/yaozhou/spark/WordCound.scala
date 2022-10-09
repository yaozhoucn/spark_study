package com.yaozhou.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/10/9 10:32
 * @Desc:
 */
object WordCound {
  def main(args: Array[String]): Unit = {

/*    //1.创建SparkConf并设置App名称，设置本地模式运行

    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口

    val sc = new SparkContext(conf)

    //3.使用sc创建RDD，输入和输出路径都是本地路径

    var line:RDD[String] = sc.textFile("input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).saveAsTextFile("output")

    //4.关闭连接

    sc.stop()*/

    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3.读取指定位置文件:hello atguigu atguigu
    val lineRdd: RDD[String] = sc.textFile("input")

    //4.读取的一行一行的数据分解成一个一个的单词（扁平化）(hello)(atguigu)(atguigu)
    val wordRdd: RDD[String] = lineRdd.flatMap(line => line.split(" "))

    //5. 将数据转换结构：(hello,1)(atguigu,1)(atguigu,1)
    val wordToOneRdd: RDD[(String, Int)] = wordRdd.map(word => (word, 1))

    //6.将转换结构后的数据进行聚合处理 atguigu:1、1 =》1+1  (atguigu,2)
    val wordToSumRdd: RDD[(String, Int)] = wordToOneRdd.reduceByKey((v1, v2) => v1 + v2)

    //7.将统计结果采集到控制台打印
    val wordToCountArray: Array[(String, Int)] = wordToSumRdd.collect()
    wordToCountArray.foreach(println)

    //wordToSumRdd.saveAsTextFile("output")

    //一行搞定
    //sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).saveAsTextFile(args(1))

    //8.关闭连接
    sc.stop()

  }

}
