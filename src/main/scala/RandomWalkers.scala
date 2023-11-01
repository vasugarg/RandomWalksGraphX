package cs441.HW2

import GraphXUtilz.GraphLoader.{EdgeData, VertexData, loadGraph}
import Utilz.CreateLogger
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.slf4j.Logger

import scala.util.Random

object RandomWalkers {
  val logger: Logger = CreateLogger(classOf[Main.type])
  type Matcher = (Int, Double)

  def run(filename: String): Array[Map[VertexId, Matcher]] = {
    val conf = new SparkConf().setAppName("Pregel Random Walk with Dynamic Matcher").setMaster("local")
    val sc = new SparkContext(conf)

    val (vertexArray, edgeArray) = loadGraph(filename)
    val startingVertices: Set[VertexId] = Random.shuffle(vertexArray.map(_.id)).take(4).toSet

    val VertexRDD: RDD[(VertexId, (VertexData, List[(VertexId, List[(Int, Double)])], Option[List[(Int, Double)]]))] =
      sc.parallelize(vertexArray.map { v =>
        val initialPath: List[(VertexId, List[(Int, Double)])] = if (startingVertices.contains(v.id)) {
          List((v.id, VertexSimilarity.calcMaxSimilarity(v))) // Initialize with self and matcher value
        } else {
          List.empty[(VertexId, List[(Int, Double)])]
        }
        (v.id, (v, initialPath, None))
      })

    VertexRDD.take(10).foreach { case (vertexId, vertexData) =>
      println(s"Vertex ID: $vertexId, Vertex Data: $vertexData")
    }
    val EdgeRDD: RDD[Edge[EdgeData]] = sc.parallelize(edgeArray.map(e => Edge(e.fromId, e.toId, e)))

    val myGraph = Graph(VertexRDD, EdgeRDD)

    def vertexProgram(id: VertexId,
                       attr: (VertexData, List[(VertexId, List[(Int, Double)])], Option[List[(Int, Double)]]),
                       msg: List[(VertexId, List[(Int, Double)])]
                     ): (VertexData, List[(VertexId, List[(Int, Double)])], Option[List[(Int, Double)]]) = {
      if (msg.nonEmpty && msg.length < 10) {
        val matcher = attr._3.getOrElse(VertexSimilarity.calcMaxSimilarity(attr._1))
        (attr._1, msg :+ (id, matcher), Some(matcher))
      } else {
        attr
      }
    }

    def sendMessage(edge: EdgeTriplet[(VertexData, List[(VertexId, List[(Int, Double)])], Option[List[(Int, Double)]]), EdgeData]
                   ): Iterator[(VertexId, List[(VertexId, List[(Int, Double)])])] = {
      if (edge.srcAttr._2.nonEmpty && edge.srcAttr._2.length < 10) {
        Iterator((edge.dstId, edge.srcAttr._2))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(
                         a: List[(VertexId, List[(Int, Double)])],
                         b: List[(VertexId, List[(Int, Double)])]
                       ): List[(VertexId, List[(Int, Double)])] = {
      if (a.length < b.length) b else a
    }

    val initialMessage: List[(VertexId, List[(Int, Double)])] = List()

    val resultGraph = myGraph.pregel(initialMessage, 20, EdgeDirection.Out)(
      (id, attr, msg) => vertexProgram(id, attr, msg),
      sendMessage,
      messageCombiner
    )
    resultGraph.vertices.filter(_._2._2.nonEmpty).collect().foreach { case (_, (_, path, _)) =>
      val pathStr = path.flatMap { case (vid, matchers) =>
        matchers.map { case (intVal, doubleVal) => s"$vid : ($intVal, $doubleVal)" }
      }.mkString(" -> ")
      println(s"$pathStr")
    }


    val resultMap: Array[Map[VertexId, Matcher]] = resultGraph.vertices.filter(_._2._2.nonEmpty).map {
      case (_, (_, path, _)) =>
        path.flatMap { case (vid, matchers) =>
          matchers.map { case (intVal, doubleVal) =>
            (vid, (intVal, doubleVal))
          }
        }
    }.collect().map(_.toMap)
    sc.stop()
    resultMap
  }
}
