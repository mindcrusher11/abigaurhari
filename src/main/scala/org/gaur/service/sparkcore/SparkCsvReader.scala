package org.gaur.service.sparkcore

import org.apache.spark.sql.DataFrame
import org.gaur.iservice.IAbFileReader
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.types.StructType
import org.gaur.abexception.AbiExceptions
import org.gaur.model.PracticeInfoModel
import org.gaur.utils.AbiConstants

/**
  * Created by gaur on 7/11/16.
  */
case class SparkCsvReader(argInputPath:String) extends IAbFileReader{

  var outputDataFrame:DataFrame = _
  val abSqlContext = AbSqlContext.getAbSqlContext()

  def getDataFrame(inputPath:String=argInputPath, headerLine:String=AbiConstants.TRUE, inferSchemaCheck:String=AbiConstants.TRUE,
                   schemaObject:StructType = null, rowFormat:String =AbiConstants.SPARK_CSV_FORMAT):DataFrame={

    if(!isFileExists(inputPath)) {
      throw new AbiExceptions(AbiConstants.FILE_NOT_EXIST)
    }
    else {
      val outDataframe = abSqlContext.read.format(rowFormat).option(AbiConstants.ROW_HEADER, headerLine) // Use first line of all files as header
      var schemaDataFrame:DataFrame = null
        if(schemaObject == null) {
            schemaDataFrame = outDataframe.option(AbiConstants.INFER_SCHEMA, inferSchemaCheck).load(inputPath)
          }else {
          // // Automatically infer data types
          schemaDataFrame = outDataframe.schema(schemaObject).load(inputPath)
        }
      outputDataFrame = schemaDataFrame
    }

    outputDataFrame
  }

  override def display(): Unit = {
    if(outputDataFrame != null) outputDataFrame.show() else println(AbiConstants.NO_RECORDS_FOUND)
  }

  override def getData(): AnyRef ={
    val str = ""
    str
  }

  def getData(inputPath:String=argInputPath,headerLine:String=AbiConstants.TRUE, inferSchemaCheck:String=AbiConstants.TRUE,schemaObject:StructType): DataFrame={
    getDataFrame(argInputPath,headerLine,inferSchemaCheck,schemaObject)
    outputDataFrame
  }

  override def getRowCount():Long={
    outputDataFrame.count()
  }

  override def isFileExists(inputPath:String=argInputPath): Boolean = {
    Files.exists(Paths.get(inputPath))
  }

}
