package com.yaozhou.spark.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *
 *  aggregateKey 按照K处理分区内和分区间逻辑
 *
 *  (1) zeroValue (初始值) :给每- - 个分区中的每一种key一个初始值;
 *  (2) seqOp (分区内) :函数用于在每一个分区中用初始值逐步迭代value
 *  (3) combOp (分区间) :函数用于合并每个分区中的结果。
 *
 */
object Spark04_Transformation_aggregateKey{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark04_Transformation_aggregateKey").setMaster("local[*]")

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

    //利用reduceByKey进行wordcount
    //rdd.reduceByKey(_+_).collect().foreach(println)

    //利用groupByKey进行wordCount

    rdd.groupByKey().map{
      case (word,datas) => {
        (word,datas.sum)
      }
    }.collect().foreach(println)

    //使用aggregate取出每个分区相同key对应值的最大值，然后相加

    rdd.aggregateByKey(0)(
      (x,y) => math.max(x, y),
      (a,b) => a + b
    )

    //简化
    //每次比对会使用初始值0与每个（key,value）value进行比对，获取最大值，然后最后将各个分区中的最大值相加
    println("=====================使用aggregateByKey======================")
    rdd.aggregateByKey(0)(
      math.max(_, _),
       _ + _
    ).collect().foreach(println)



    sc.stop()
  }


}

