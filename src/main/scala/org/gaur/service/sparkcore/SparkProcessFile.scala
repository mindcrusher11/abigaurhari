package org.gaur.service.sparkcore

import org.apache.spark.sql.{Column, DataFrame}
import org.gaur.iservice.{IAbFileReader, IProcessFile}
import org.gaur.utils.{AbiConstants, FileReaderFactory}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{length, trim, when}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
  * Created by gaur on 7/11/16.
  */
class SparkProcessFile extends IProcessFile{

  def getAsDataReader(argInputPath:String): SparkCsvReader ={
    val outputRows = FileReaderFactory.getFileReader(argInputPath,AbiConstants.SPARK_READER).asInstanceOf[SparkCsvReader]
    outputRows
  }

  def getAsDataFrame(argInputPath:String,schemaType:StructType=null):DataFrame={
    val dataReader = getAsDataReader(argInputPath)
    val outData = dataReader.getData(argInputPath,AbiConstants.TRUE,AbiConstants.FALSE,schemaType).asInstanceOf[DataFrame]
    outData
  }

  def joinDataFrame(leftDataFrame:DataFrame, rightDataFrame:DataFrame, columnNames:Seq[String],joinType:String=AbiConstants.INNER_JOIN): DataFrame = {
      leftDataFrame.join(broadcast(rightDataFrame),columnNames,joinType)
  }

  def joinFiles(leftFilePath:String,rightFilePath:String,columnNames:Seq[String], joinType:String= AbiConstants.INNER_JOIN):DataFrame ={
      val leftDataFrame = getAsDataFrame(leftFilePath)
      val rightDataFrame = getAsDataFrame(rightFilePath)
      joinDataFrame(leftDataFrame, rightDataFrame,columnNames,joinType)
  }

  def getColumnValueCount(inputData:AnyRef,columnName:String,colValue:String):Long = {
    val groupDataFrame = inputData.asInstanceOf[DataFrame].groupBy(columnName).count()
    val columnExpression = columnName + " = '" + colValue + "'"
    val filteredRow = groupDataFrame.select(trim(groupDataFrame(columnName)).alias(columnName),groupDataFrame("count")).filter(columnExpression)
    filteredRow.select("count").head.getLong(0)
  }

  def getItemAvgInColumn(inputData:AnyRef, columnName:String, filterValue:String, avgColumnName:String):Double={
      val filterExpression = columnName + " = '" + filterValue + "'"
      val inputDataFrame = inputData.asInstanceOf[DataFrame]
      val filteredDataFrame = inputDataFrame.asInstanceOf[DataFrame].select(trim(inputDataFrame(columnName)).alias(columnName),inputDataFrame(avgColumnName)).filter(filterExpression)
      val avgValue = filteredDataFrame.select(avg(avgColumnName)).head().getDouble(0)
      avgValue
  }

  def getTopNItemsAggregate(inputDataFrame:AnyRef, groupByColName:String, topNItems:Int, aggregateColName:String):Array[Row] ={
    val topItems =  inputDataFrame.asInstanceOf[DataFrame].groupBy(groupByColName).agg(sum(aggregateColName).alias(aggregateColName)).sort(desc(aggregateColName)).take(topNItems)
    topItems
  }


}
