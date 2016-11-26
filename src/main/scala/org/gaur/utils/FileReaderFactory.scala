package org.gaur.utils

import org.gaur.iservice.IAbFileReader
import org.gaur.service.sparkcore.SparkCsvReader

/**
  * Created by gaur on 7/11/16.
  */
object FileReaderFactory {

  def getFileReader(inputFilePath:String,readerType:String):IAbFileReader = {
        readerType match {
          case AbiConstants.SPARK_READER => new SparkCsvReader(inputFilePath)
          case default => null
        }
  }


}
