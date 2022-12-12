package com.yaozhou.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *
 *  combineKey
 *
 */
object Spark06_Transformation_combineKey{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark06_Transformation_combineKey").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    //创建kv形式的
    val rdd = sc.makeRDD(List(("xiaowang", 45), ("xiaoli", 87), ("xiaowang", 78), ("xiaoli", 3), ("xiaowang", 98), ("xiaoli", 99)),2)

    //需求：求每一门成绩的平均值

    //方案1：groupByKey
    val groupRdd: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    //如果分组之后，某一组的数据量比较大，会造成单点压力；
    groupRdd.map{
      case (name,datas) => {
        (name,datas.sum/datas.size)
      }
    }.collect().foreach(println)


    //方案2：使用reduceByKey求平均值
    //第一步：对数据进行结构转换（“xiaowng”，（45，1））
    val mapRdd: RDD[(String, (Int, Int))] = rdd.map {
      case (name, score) => {
        (name, (score, 1))
      }
    }
    //使用reduceByKey进行聚合

    val reduceRdd: RDD[(String, (Int, Int))] = mapRdd.reduceByKey {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    }

    //数据结构转化映射
//    reduceRdd.map{
//      case (name,(score,count)) => {
//        (name,score/count)
//      }
//    }.collect().foreach(println)


    //方案3：使用combineKey求平均值
    /**
     * createCombiner:对Rdd中当前key取出第一个value做一个初始化 c
     * mergeValue: 分区内计算规则，主要在分区内进行，将当前分区的value值，合并到初始化得到的C上面
     * mergeCombiners: 分区间的计算规则
     */
    val combineRdd: RDD[(String, (Int, Int))] = rdd.combineByKey(
      //参数1：createCombiner
      ((a: Int) => {
        (a, 1)
      }),
      //参数2：mergeValue
      (
        (tup1: (Int, Int), value) => {
          (tup1._1 + value, tup1._2 + 1)
        }),
      //参数3:mergeCombiners
      (tup2: (Int, Int), tup3: (Int, Int)) => {
        (tup2._1 + tup3._1, tup2._2 + tup3._2)
      })
    //数据格式转换映射求平均值
    combineRdd.map {
      case ((name), (score, count)) => {
        (name, score / count)
      }
    }.collect().foreach(println)


    sc.stop()
  }


}

