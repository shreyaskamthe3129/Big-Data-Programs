package edu.uta.cse6331

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Source {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ShortestPath");
    conf.setMaster("local[*]");

    val sparkContextObject = new SparkContext(conf);

    val graphCoordinates = sparkContextObject.textFile(args(0)).map(line => {
      val coordinateLine = line.split(",");
      val ithNode = coordinateLine(0).toLong;
      val distance = coordinateLine(1).toLong
      val jthNode = coordinateLine(2).toLong;

      (ithNode, distance, jthNode)

    })

    val jTripleDestEdges = graphCoordinates.groupBy(graphTuple => {
      (graphTuple._3)
    }).map(groupByTuple => {
      (groupByTuple._1, groupByTuple._2,
        (if (groupByTuple._1 == 0) {
          0L
        } else {
          Long.MaxValue
        }))
    })

    var iDistList = graphCoordinates.map(tuple => {
      (tuple._1, (if (tuple._1 == 0L) {
        0L
      } else {
        Long.MaxValue
      }), tuple._2, tuple._3)
    })

    var finalResultRDD = graphCoordinates.map(tuple => {
      (tuple._1, tuple._2)
    })
    
    var compareDistResult = jTripleDestEdges.map(tuple => {
      (tuple._1,tuple._3)
    })

    for (counter <- 1 to 4) {

      iDistList = iDistList.
        map(iTuple => {
          (iTuple._4, iTuple)
        }).join(compareDistResult.
          map(jTuple => (jTuple._1, jTuple))).
        map {
          case (jkey, (iTuple, jTuple)) => (iTuple._1, jTuple._2, iTuple._3, iTuple._4)
        }
      
      compareDistResult = iDistList.map(newDistTuple => {
        (newDistTuple._1,newDistTuple)
      }).join(compareDistResult.map(jTuple => {
        (jTuple._1, jTuple)
      })).map( {
        case (jkey,(newDistTuple, jTuple)) =>if((jTuple._2 != Long.MaxValue)&&(newDistTuple._2 > (jTuple._2 + newDistTuple._3)) )
                                                (newDistTuple._4, jTuple._2 + newDistTuple._3)
                                             else
                                                 (newDistTuple._4, newDistTuple._2)

      }).reduceByKey(_ min _)
      
    }
    
    compareDistResult.collect().filter(tuple => {tuple._2 != Long.MaxValue}).
                toSeq.sortBy(tuple => {tuple._1}).foreach(println)

  }

}