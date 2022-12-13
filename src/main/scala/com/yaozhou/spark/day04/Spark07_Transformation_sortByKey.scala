package com.yaozhou.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: HANG
 * @Date: 2022/12/9 12:20
 * @Desc:
 *
 *  sortByKey使用自定义的排序规则，则必须继承  Ordered
 *
 */
object Spark07_Transformation_sortByKey{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark06_Transformation_combineKey").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(List((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))

    //默认降序升序
    //rdd.sortByKey().collect().foreach(println)

    //降序排序
    //rdd.sortByKey(false).collect().foreach(println)


    //自定义排序
    val studentList = List(
      (new Student("xiaowang", 18), 2),
      (new Student("xiaoli", 17), 2),
      (new Student("xiaowang", 19), 2),
      (new Student("xiaonuan", 8), 2))
    val stuRdd: RDD[(Student, Int)] = sc.makeRDD(studentList)
    val sortRdd: RDD[(Student, Int)] = stuRdd.sortByKey()
    sortRdd.collect().foreach(println)
    sc.stop()
  }


}

class Student(var name:String,var age:Int) extends Ordered[Student] with Serializable{
  //指定比较规则
  override def compare(that: Student): Int = {

    //先按照姓名排序升序，如果名称相同的话，再按照年龄进行降序排序
    var res: Int = that.name.compareTo(this.name)
    if (res == 0){
      res = that.age.compareTo(this.age)
    }
    res
  }

  override def toString = s"Student($name, $age)"
}

