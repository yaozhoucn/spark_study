package com.yaozhou.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/11/29 12:20
 * @Desc: 行动算子
 */
object Spark01_action {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark01_action").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)


    //1.reduce f函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 6, 5, 8),8)
    val res: Int = rdd.reduce(_ + _)

    //2.collect()以数组的形式返回数据集

    //rdd.collect().foreach(println)

    //3.count 返回Rdd中的元素个数

    //println(rdd.count())

    //4.first()返回RDD中的第一个元素

    //println(rdd.first())

    //5.take()返回由RDD前n个元素组成的数组

    //rdd.take(3).foreach(println)

    //6.rdd的foreach:不同的executon分别执行foreach执行
    //数组的foreach是把数据发送到executon，在driver段执行foreach

    //7.takeOrdered先对rdd的数据进行排序，然后获取前N个,默认升序排序

    println(rdd.takeOrdered(3).mkString(","))

    //8.aggregate  赋值一个初始值，然后分区内与初始值比较，然后分区间也与初始值比较,最后初始值也参与相加

    //println(rdd.aggregate(10)(_ + _, _ + _))

    //println(rdd.aggregate(0)(_ + _, _ + _))

    //aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U


    //9.fold 是aggregate的简化，分区内与分区间的计算方法相同

    //println(rdd.fold(10)(_ + _))

    //10.countByKey
    val rdd2: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))

    println(rdd2.countByKey())

    //save相关的算子

    //1.保存为文本文件
    rdd.saveAsTextFile("-----------------")
    sc.stop()
  }

}
