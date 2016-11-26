package org.gaur.utils

import java.io.File

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.gaur.model.PracticeInfoModel

import scala.reflect.runtime.{currentMirror => m, universe => ru}
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import scala.reflect.api.TypeTags

/**
  * Created by gaur on 17/11/16.
  */
object Utility {

  def getSparkSchema[A:TypeTag]:StructType = {
    val practiceSchema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    practiceSchema
  }

  def getFirstSplitElement(inputValue:String,delimiter:String,elementIndex:Int):String ={
    inputValue.split(delimiter)(elementIndex)
  }

  def getConfigInstance(configPath:String=AbiConstants.CONFIG_PATH):Config = {
    val confInfo = ConfigFactory.parseFile(new File(configPath))
    confInfo
  }
}

