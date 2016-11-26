package org.gaur.service

import javax.swing.text.Utilities

import com.typesafe.config.Config
import org.junit.Test
import junit.framework.TestCase
import org.apache.spark.sql.DataFrame
import org.gaur.service.sparkcore.{SparkCsvReader, SparkProcessFile}
import org.junit.Assert._
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.gaur.model.{PracticeInfoModel, PrescriptionModel}
import org.gaur.utils.Utility

/**
  * Created by gaur on 7/11/16.
  */
class SparkProcessTests extends TestCase{

  var sparkProcess:SparkProcessFile = _
  var sparkDataFrame:DataFrame = _
  var sparkCsvReader:SparkCsvReader = _
  var confInfo:Config= _

  override def setUp(): Unit = {
    confInfo = Utility.getConfigInstance()
    sparkProcess = new SparkProcessFile
    sparkCsvReader = new SparkCsvReader(confInfo.getString("file_path.practiceFilePath"))
    sparkDataFrame = sparkCsvReader.getDataFrame()
  }

 def testRowCount(): Unit = {
    assertEquals(sparkDataFrame.count(),10074)
  }
  /*
  def testFileExist:Unit = {
    println("inside test file exist testcase")
    assertEquals(sparkProcess.getAsDataReader().isFileExists(),true)
  }

  def testDisplay:Unit={
    println("inside testdiplay testcase")
    sparkProcess.getAsDataFrame().show()
    assertEquals(sparkDataFrame.count(),10073)
  }*/

 /* def testPracticesCountInCounty():Unit={
    val prescriptionDataFrame = sparkProcess.getAsDataFrame("/home/gaur/Downloads/T201201PDP IEXT.CSV").filter("PRACTICE is not null")
    val practiceDataFrame = sparkProcess.getAsDataFrame("/home/gaur/Downloads/T201202ADD REXT (2).CSV")
    prescriptionDataFrame.count()
    //val joinedDataFrame =  sparkProcess.joinFiles("/home/gaur/Downloads/T201201PDP IEXT.CSV","/home/gaur/Downloads/T201202ADD REXT (2).CSV",Seq("PRACTICE"))
    //val joinedDataFrame = sparkProcess.joinDataFrame(prescriptionDataFrame, practiceDataFrame,Seq("PRACTICE"),"LeftOuter")
    val valueCount = sparkProcess.getColumnValueCount(practiceDataFrame,"County","LONDON")
    println("number of practices in London are :- " + valueCount)
    //print( prescriptionDataFrame.count() + " "+practiceDataFrame.count() + " "+ joinedDataFrame.count())
    //joinedDataFrame.printSchema()
    assert(true)
  }*/

 /* def testAvgColumn():Unit={
    val prescriptionDataFrame = sparkProcess.getAsDataFrame("/home/gaur/Downloads/T201201PDP IEXT.CSV").filter("PRACTICE is not null")
    val avgValue = sparkProcess.getItemAvgInColumn(prescriptionDataFrame,"BNFNAME","Peppermint Oil","ACTCOST")
    println(avgValue)
    assert(avgValue.equals(49.25632027257238))
  }*/





  def testTotalPracticesInCounty():Unit={
    val practiceDataFrame = sparkProcess.getAsDataFrame(confInfo.getString("file_path.practiceFilePath"),Utility.getSparkSchema[PracticeInfoModel])
    val valueCount = sparkProcess.getColumnValueCount(practiceDataFrame,"county","LONDON")
    println("=========================================================================================")
    println("number of practices in London are :- " + valueCount)
    println("=========================================================================================")
    assert(valueCount.equals(754L))
  }

  def testAvgColumn():Unit={
    val prescriptionDataFrame = sparkProcess.getAsDataFrame(confInfo.getString("file_path.prescriptionFilePath"),Utility.getSparkSchema[PrescriptionModel]).filter("practiceId is not null")
    val avgValue = sparkProcess.getItemAvgInColumn(prescriptionDataFrame,"bnfName","Peppermint Oil","actualCost")
    println("=========================================================================================")
    println("average value of peppermint oil is :- " + avgValue)
    println("=========================================================================================")
    assert(avgValue.equals(52.85308086560365))
  }

  def testTopItems():Unit={
    val prescriptionDataFrame = sparkProcess.getAsDataFrame(confInfo.getString("file_path.prescriptionFilePath"),Utility.getSparkSchema[PrescriptionModel]).filter("practiceId is not null")
    val practiceDataFrame = sparkProcess.getAsDataFrame(confInfo.getString("file_path.practiceFilePath"),Utility.getSparkSchema[PracticeInfoModel]).filter("practiceId is not null")
    val joinedDataFrame = sparkProcess.joinDataFrame(prescriptionDataFrame, practiceDataFrame,Seq("practiceId"),"LeftOuter")
    val topItems = sparkProcess.getTopNItemsAggregate(joinedDataFrame,"postCode",5,"actualCost")
    println("=========================================================================================")
    println("top five items according to postcode and actualcost")
    topItems.map(row => println(row))
    println("=========================================================================================")
    assert(topItems.head.getString(0).equals("SK11 6JL"))
  }

  def testRegionsItems():Unit={
    val prescriptionDataFrame = sparkProcess.getAsDataFrame(confInfo.getString("file_path.prescriptionFilePath"),Utility.getSparkSchema[PrescriptionModel]).filter("practiceId is not null")
    val practiceDataFrame = sparkProcess.getAsDataFrame(confInfo.getString("file_path.practiceFilePath"),Utility.getSparkSchema[PracticeInfoModel]).filter("practiceId is not null")
    val regionsDataFrame = sparkProcess.getAsDataFrame(confInfo.getString("file_path.ukPostCodeFilePath"))
    //val getFirstSplitElement: ((String,String,Int) => String) = (inputValue:String,separator:String,stringIndex:Int) => {inputValue.split(separator)(stringIndex)}
    val getFirstSplitElement: (String => String) = (inputValue:String) => {inputValue.split(" ")(0)}
    val splitUdf = udf(getFirstSplitElement)
    val splitPostCode = practiceDataFrame.withColumn("district",splitUdf(practiceDataFrame("postCode")))
    val postCodeRegion = sparkProcess.joinDataFrame(splitPostCode, regionsDataFrame,Seq("district"),"LeftOuter")
    val joinedDataFrame = sparkProcess.joinDataFrame(prescriptionDataFrame, postCodeRegion,Seq("practiceId"),"inner")

    val filterFluclolaxicilin : (String => Boolean) = (inputValue:String) => {if(inputValue.contains("Flucloxacillin") && !inputValue.contains("Co-Fluampicil")) true else false}
    val fluclolaxicilinUdf = udf(filterFluclolaxicilin)
    val fluclolaxicilinData = joinedDataFrame.filter(fluclolaxicilinUdf(col("bnfName")))
    val regionMean = fluclolaxicilinData.groupBy("uk_region").agg(avg("actualCost").alias("avgCost"))
    println("=========================================================================================")
    println("average cost of flucloxaclin by region")
    regionMean.show()
    println("=========================================================================================")
    var nationalMean = fluclolaxicilinData.agg(avg("actualCost").alias("nationalMean")).head().getDouble(0)
    println("=========================================================================================")
    println("national mean is " + nationalMean)
    println("=========================================================================================")

    val deviationFromMean:(Double => Double) = (inputMean:Double) => {inputMean - nationalMean}
    val meanDeviationUdf = udf(deviationFromMean)
    println("=========================================================================================")
    println("deviation from national mean")
    val output = regionMean.withColumn("deviation",meanDeviationUdf(col("avgCost")))
    output.show()
    println("=========================================================================================")
    assert(output.head().getDouble(1).equals(191.07))
  }
}
