package org.gaur.service

import junit.framework.TestCase
import org.apache.spark.sql.types.StructType
import org.gaur.model.PrescriptionModel
import org.gaur.utils.Utility

/**
  * Created by gaur on 20/11/16.
  */
class UtilityTest extends TestCase{

  def testSplitElement():Unit={
      assert(Utility.getFirstSplitElement("testing value"," ",0).equals("testing"))
  }

  def testSparkSchema():Unit={
    assert(Utility.getSparkSchema[PrescriptionModel].isInstanceOf[StructType])
  }
}
