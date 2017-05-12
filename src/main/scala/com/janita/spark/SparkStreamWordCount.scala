package com.janita.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Janita on 2017-05-12 15:28
  * 用Spark Streaming实现实时WordCount
  */
object SparkStreamWordCount {


  /**
    * 在linux上使用nc命令向某个端口发送数据
    * 1.使用命令：yum install nc安装nc
    * 2.nc -l 9999 ：发送消息
    * 3.可以看控制台输出
    * @param args
    */
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置为本地模式运行,local[2]代表开两个线程
    val conf = new SparkConf().setMaster("local[2]").setAppName("Janita")

    //设置DStream批次时间间隔为2秒
    val ssc = new StreamingContext(conf,Seconds(2))

    //通过网络读取数据
    val lines = ssc.socketTextStream("192.168.128.101",9999)

    //将读到的数据用空格切成单词
    val words = lines.flatMap(_ . split(" "))

    //将单词和1组成一个pair
    val pairs = words.map(word => (word, 1))

    //按单词进行分组求相同单词出现的次数
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
