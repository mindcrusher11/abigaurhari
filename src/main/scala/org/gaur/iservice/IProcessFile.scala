package org.gaur.iservice

/**
  * Created by gaur on 20/11/16.
  */
trait IProcessFile {
  def joinFiles(leftFilePath:String,rightFilePath:String,columnNames:Seq[String], joinType:String):AnyRef
  def getColumnValueCount(inputDataFrame:AnyRef,columnName:String,colValue:String):Long
  def getItemAvgInColumn(inputDataFrame:AnyRef, columnName:String, filterValue:String, avgColumnName:String):Double
  def getTopNItemsAggregate(inputDataFrame:AnyRef, groupByColName:String, topNItems:Int, aggregateColName:String):AnyRef
}
