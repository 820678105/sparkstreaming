package com.qf.sparkstreamingwc

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Description 整合hdfs文件系统
 * @Author Zhang Kun
 * @Date 2020-06-09 11:33
 */
object HDFSSparkStreaming {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("hdfs").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(2))
    //加载外部文件
    val linesDS: DStream[String] = ssc.textFileStream("")
    //打印
    linesDS.print()
    //启动
    ssc.start()
    //等待停止
    ssc.awaitTermination()

  }
}
