package com.xipu.ReadFromKafka

import scala.collection.JavaConversions._
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types._
import org.apache.log4j.{ Level, Logger }
import scala.reflect.runtime.universe
import com.google.gson.Gson
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringDeserializer

object Kafka2Spark2Activate {
  Logger.getLogger("org").setLevel(Level.ERROR) //设置日志级别
  //var confPath: String = System.getProperty("user.dir") + File.separator + "conf/0285.properties"

  /**
   * 建表Schema定义
   */
  val kuduTableSchema = StructType(
    //  col  name  type  nullable
    StructField("id", IntegerType, false) ::
      StructField("game_id", IntegerType, true) ::
      StructField("device_id", IntegerType, true) ::
      StructField("ip", StringType, true) ::
      StructField("channel", StringType, true) ::
      StructField("kid", StringType, true) ::
      StructField("platform", StringType, true) ::
      StructField("created_at", StringType, true) ::
      StructField("updated_at", StringType, true) ::
      StructField("deleted_at", StringType, true) :: Nil)
  /**
   * 定义一个Activate对象
   */
  case class Activate(
    id: Int,
    game_id: Int,
    device_id: Int,
    ip: String,
    channel: String,
    kid: String,
    platform: String,
    created_at: String,
    updated_at: String,
    deleted_at: String)
    def main(args: Array[String]): Unit = {
      val topics = "activate"
    val brokers = "xipuhadoop:9092"
    val kuduMaster = "xipuhadoop:7051"
    val topicsSet = topics.split(",").toSet
    //spark配置信息
    val sparkConf = new SparkConf().setMaster("local").setAppName("Kafka2Spark2Activate")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    //设置检查点
    ssc.checkpoint("hdfs://xipuhadoop:8020/ck/activate")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers //zk节点地址
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "group.id" -> "testgroup" //设置用户组
      , "auto.offset.reset" -> "latest" //设置偏移量为最后
      , "enable.auto.commit" -> "false" //设置自动提交为false
      )
    //直连卡夫卡采取Receive_base方式
    val dStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    //导入隐式 
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val kuduContext = new KuduContext(kuduMaster, sc)
    //判断表是否存在
    if (!kuduContext.tableExists("activate")) {
      println("create Kudu Table :{activate}")
      val createTableOptions = new CreateTableOptions()
      createTableOptions.setNumReplicas(1).addHashPartitions(List("id"), 8)
      kuduContext.createTable("activate", kuduTableSchema, Seq("id"), createTableOptions)
    }

    def handleMessage2CaseClass(jsonStr: String): Activate = {
      //使用case class 处理json@gson
      val gson = new Gson()
      gson.fromJson(jsonStr, classOf[Activate])
    }

    dStream.map(record => handleMessage2CaseClass(record.value())).foreachRDD(rdd => {
      //json内容转化为DF，并写入kudu的activate表
      val ActivateDF = sqlContext.createDataFrame(rdd)
      kuduContext.upsertRows(ActivateDF, "activate")

    })
    //手动提交偏移量
    dStream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      dStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
    //开关
    ssc.start()
    ssc.awaitTermination()
    }
    
}