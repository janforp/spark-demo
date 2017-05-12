package com.janita.spark

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Janita on 2017-05-12 16:09
  */
object KafkaSparkStream {

  val updateFunc = (iterator : Iterator[(String,Seq[Int], Option[Int])]) => {
    iterator.flatMap{case(x,y,z) => Some(y.sum + z.getOrElse(0)).map(n => (x, n))}
  }

  def main(args: Array[String]): Unit = {

    val Array(zkQuorum,groupId,topics,numThreads,hdfs) = args

    val conf = new SparkConf().setAppName("Janita")

    val ssc = new StreamingContext(conf,Seconds(2))

    ssc.checkpoint(hdfs)

    //设置topic信息
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    //从Kafka中拉取数据创建DStream
    val lines = KafkaUtils.createStream(ssc,zkQuorum,groupId,topicMap,StorageLevel.MEMORY_AND_DISK).map(_._2)

    val urls = lines.map(x =>(x.split(" ")(6),1))

    val result = urls.updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    result.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
