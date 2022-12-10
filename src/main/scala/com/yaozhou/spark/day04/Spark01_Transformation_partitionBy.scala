package com.yaozhou.spark.day04

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *
 *  partitionBy 根据key对元素进行重新分区
 *
 *  RDD本身是没有partitionBy这个算子的，通过隐式转换动态给kv类型的RDD扩展的功能
 *
 */
object Spark01_Transformation_partitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark01_Transformation_partitionBy").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    //创建kv形式的
    val rdd = sc.makeRDD(List((1, "aaa"), (2, "bbb"), (3, "ccc")),3)


    println("====================重新分区之前=====================")

    rdd.mapPartitionsWithIndex{
      (index,datas) => {
        println(index+"=========>"+datas.mkString(","))
        datas
      }
    }.collect().foreach(println)

    //val newRdd = rdd.partitionBy(new HashPartitioner(2))

    //使用自己创建的分区规则进行分区
    val newRdd = rdd.partitionBy(new MyPartition(4))

    println("====================重新分区之后=====================")

    newRdd.mapPartitionsWithIndex{
      (index,datas) => {
        println(index+"=========>"+datas.mkString(","))
        datas
      }
    }.collect().foreach(println)

    sc.stop()
  }
  //更具业务规则自己创建分区

}
//自定义分区器
class MyPartition(partitions: Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
//    val str = key.asInstanceOf[String]
//
//    if(str.startsWith("1")){
//      0
//    }else if (str.startsWith("2")){
//      1
//    }else{
//      2
//    }
    if (key == 1){
      0
    }else if(key == 2){
      1
    }else{
      2
    }
  }
}
