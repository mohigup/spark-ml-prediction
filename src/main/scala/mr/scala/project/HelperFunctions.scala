package mr.scala.project

import scala.collection.mutable.ListBuffer

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * This class provides helper functions to other driver classes.
 */
object HelperFunctions {

  /**
   * This method parses the given line of labeled data set and generates the 
   * LabeledPoint for it.
   */
  def getLabeledPoint(line: String): Array[LabeledPoint] =
    {
      val lpBuf = new ListBuffer[LabeledPoint]

      if (null != line && !line.contains("SAMPLING_EVENT_ID")) 
      {
        lpBuf += parseLineToLabeledPoint(line, true)
      }

      return lpBuf.toArray
    }

  /**
   * This method parses the given line of unlabeled data set and generates the 
   * LabeledPoint for it. SampleId is stored as a label double value. 
   */
  def getUnlabeledPoint(line: String): Array[LabeledPoint] =
    {
      val lpBuf = new ListBuffer[LabeledPoint]

      if (null != line && !line.contains("SAMPLING_EVENT_ID")) 
      {
        lpBuf += parseLineToLabeledPoint(line, false)
      }

      return lpBuf.toArray
    }

  /**
   * This method parses the line of labeled or unlabeled data set to LabeledPoint.
   */
  def parseLineToLabeledPoint(line: String, isLabeledData: Boolean): LabeledPoint =
    {
      // features considered
      val features = Array(0,1,2,3)//Hidden
      val columns = line.split(",")

      var label = 0.0

      // store actual label for labeled data set and sample id in case of unlabeled data set
      if (isLabeledData) {
        if (columns(26).equals("X") || columns(26).toDouble > 0.0) {
          label = 1.0
        }
      } else {
        label = columns(0).substring(1).toDouble
      }

      // indices and values for sparse vector generation
      val indices = new ListBuffer[Int]
      val values = new ListBuffer[Double]

      for (index <- 0 until features.length) {
        val colIndex = features(index)
        
        if (null != columns(colIndex) && !"".equals(columns(colIndex)) && !"?".equals(columns(colIndex)) && (!"X".equalsIgnoreCase(columns(colIndex)))) {
          indices += index

          
          if (colIndex == //Hidden) 
          {
            values += (columns(colIndex).toDouble - 1.0)
          } 
        
          else if (colIndex == //Hidden) 
          {
            if (columns(colIndex).equalsIgnoreCase("P")) {
              values += 0.0
            } else if (columns(colIndex).equalsIgnoreCase("P")) {
              values += 1.0
            } else if (columns(colIndex).equalsIgnoreCase("P")) {
              values += 2.0
            } else if (columns(colIndex).equalsIgnoreCase("P")) {
              values += 3.0
            } else {
              values += 4.0
            }
          } 
          else if (colIndex == //Hidden) 
          {
            var colVal = 0.0

            if (columns(colIndex).equals("X") || columns(colIndex).toDouble > 0.0) {
              colVal = 1.0
            }

            values += colVal
          } 
          // other continuous features
          else 
          {
            values += columns(colIndex).toDouble
          }
        }
      }

      return LabeledPoint(label, Vectors.sparse(features.length, indices.toArray, values.toArray))
    }
}