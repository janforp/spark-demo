package com.janita.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Janita on 2017-05-12 14:46
  *
  *  而flatMap函数则是两个操作的集合——正是“先映射后扁平化”：
  *  操作1：同map函数一样：对每一条输入进行指定的操作，然后为每一条输入返回一个对象
  *  操作2：最后将所有对象合并为一个对象
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WC").setMaster("local")
    val sc = new SparkContext(conf)

    val input = "file:///D:\\local.txt"

    val output = "file:///D:\\result"

    sc.textFile(input)  //从文件中读数据
      .flatMap(_.split(" "))  //
      .map((_, 1))  //把word - 1
      .reduceByKey(_+_, 1)  //根据word合并
      .sortBy(_._2, false)  //
      .saveAsTextFile(output) //保存到文件夹

    sc.stop()
  }

}
