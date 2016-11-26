package org.gaur.iservice

/**
  * Created by gaur on 7/11/16.
  */
trait IAbFileReader {
  def display
  def getData:AnyRef
  def getRowCount:Long
  def isFileExists(inputPath:String = ""):Boolean
}
