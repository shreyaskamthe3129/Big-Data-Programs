package edu.uta.cse6331

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Multiply {

  def parseMMatrix(line: String) = {

    val matrixFields = line.split(",");

    val ithIndex = matrixFields(0);
    val jthIndex = matrixFields(1);
    val actualValue = matrixFields(2);

    (ithIndex, jthIndex, actualValue)
  }

  def parseNMatrix(line: String) = {

    val matrixFields = line.split(",");

    val jthIndex = matrixFields(0);
    val kthIndex = matrixFields(1);
    val actualValue = matrixFields(2);

    (jthIndex, kthIndex, actualValue)
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MatrixMultiply");
    conf.setMaster("local[*]");

    val sparkContextObject = new SparkContext(conf);

    val matrixMLines = sparkContextObject.textFile(args(0));
    val matrixMMaps = matrixMLines.map(parseMMatrix);

    val matrixNLines = sparkContextObject.textFile(args(1));
    val matrixNMaps = matrixNLines.map(parseNMatrix);

    val joinedMatrix = matrixMMaps.map(mElement => (mElement._2, mElement)).join(matrixNMaps.map(nElement => (nElement._1, nElement))).map {case (k,(mElement,nElement)) => ((mElement._1.toInt,nElement._2.toInt),mElement._3.toDouble * nElement._3.toDouble) };
    val aggregateMatrix = joinedMatrix.reduceByKey((x,y) => x + y).sortByKey(true, 0);
    
    aggregateMatrix.foreach(println);
    sparkContextObject.stop();
  }

}
  
