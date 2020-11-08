package com.qf.sparkstreamingwc

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount{

  def main(args: Array[String]): Unit = {
    //创建sparkstreamming执行入口
    //local不能这么写，因为这代表一个线程进行工作中，但我们的实时流程序
    //需要有个线程去拉取数据，另一个线程去计算数据，所以至少有两个线程
    //才能完成处理
    //上传集群则把master去掉

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("wc")
    //加载配置参数，设置批次提交任务的时间间隔
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    //获取衣蛾输入的流(Dstream),代表一个输入源（kafka、socket等）
  val linesDstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop01",6666,StorageLevel.MEMORY_ONLY)
  //处理输入流数数据，编写wc的代码逻辑即可
    val words: DStream[String] = linesDstream.flatMap(_.split(" "))
    val tuples: DStream[(String, Int)] = words.map((_,1))
    val reduce: DStream[(String, Int)] = tuples.reduceByKey(_+_)
    //输出数据
    reduce.print()
    //实时流需要穹顶，和离线不一样
    ssc.start()
    //等待停止
    ssc.awaitTermination()
	//
  }
}