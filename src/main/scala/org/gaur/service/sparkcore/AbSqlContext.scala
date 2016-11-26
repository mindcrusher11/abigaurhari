package org.gaur.service.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.gaur.utils.{AbiConstants, Utility}
/**
  * Created by gaur on 7/11/16.
  */
object AbSqlContext {

  val abSqlContext = new SQLContext(getSparkContext())


  def getAbSqlContext(): SQLContext ={
    abSqlContext
  }

  def getSparkContext():SparkContext={
    val confInfo = Utility.getConfigInstance()
    val sConf = new SparkConf().setMaster(confInfo.getString("spark_kafka.masterUrl")).setAppName(confInfo.getString("spark_kafka.appName"))
    val abSparkContext = new SparkContext(sConf)
    abSparkContext.setLogLevel("ERROR")
    abSparkContext
  }

}
