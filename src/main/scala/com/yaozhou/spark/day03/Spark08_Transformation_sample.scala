package com.yaozhou.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/11/29 12:20
 * @Desc:
 */
object Spark08_Transformation_sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark08_Transformation_sample").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 10)

    /**
     withReplacement: Boolean, 是否抽样放回 true 放回 ，false 不放回
      fraction: Double, --即每个元素出现的概率
        --withReplacement=true 表示期望每一个元素的出现的次数 >0
        --withReplacement=false 表示RDD中每一个元素出现的次数概率[0,1]
      seed:抽样算法的初始值，一般不需要指定
     */


    //抽样放回
    val sampleRdd: RDD[Int] = rdd.sample(true, 1)
    //sampleRdd.collect().foreach(println)


    //抽样不放回
    val rdd2: RDD[Int] = rdd.sample(false, 0.7)
    //rdd2.foreach(println)

    val nameRdd: RDD[String] = sc.makeRDD(List("于秩晨", "李嘉腾", "谭雁成", "郑欣", "赵跃", "卢凯旋", "许耀", "郝俊杰", "间晨", "刘野", "王子文", "赵玉冬", "刘振飞", "于雪峰", "王强", "韩琦",
      "李斌", "马中渊", "王天临", "石哲", "胡金泊", "徐斐", "李海峰", "王高鹏", "丁晨", "张浩然",
      "郭伟", "竹永亮", "张嘉峰", "廖保林", "罗翔", "夏瑞浩", "王振伟", "方旭独", "张薄薄", "董佳琳", "刘俊梵", "赵煜", "毛兴达", "周奇巍", "马静", "刘阳", "刘昊", "何成江", "曹震", "李小龙", "郑玄毅"))

    val tsRdd: Array[String] = nameRdd.takeSample(false, 4)

    tsRdd.foreach(println)



    sc.stop()
  }

}
