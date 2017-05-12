package com.janita.spark

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Janita on 2017-05-12 15:51
  */
object NetWordUpdateStateWordCount {

  /**
    * String : 单词 hello
    * Seq[Int] ：单词在当前批次出现的次数
    * Option[Int] ： 历史结果
    */
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    iter.flatMap{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetWordUpdateStateWordCount")

    val ssc = new StreamingContext(conf, Seconds(5))

    //会把这些数据保存到此文件夹中
    ssc.checkpoint("d://aaa")

    val lines = ssc.socketTextStream("192.168.128.101",9999)

    val results = lines.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    results.print()
    ssc.start()
    ssc.awaitTermination()

  }


}
