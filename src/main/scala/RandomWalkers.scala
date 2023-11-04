package cs441.HW2

import GraphXUtilz.GraphLoader.{EdgeData, VertexData, loadGraph}
import org.apache.log4j.{ConsoleAppender, Level, LogManager, Logger, SimpleLayout}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.util.{Failure, Random, Success, Try}

object RandomWalkers {
  System.setProperty("log4j.configuration", "log4j.properties")
  val logger = LogManager.getRootLogger
  logger.setLevel(Level.INFO)
  logger.setLevel(Level.WARN)
  logger.setLevel(Level.DEBUG)
  logger.setLevel(Level.ERROR)

  type Matcher = (Int, Double)

  def run(filename: String): Array[Map[VertexId, Matcher]] = {
    //logger.info("Initializing Spark Context")
    val conf = new SparkConf().setAppName("Pregel Random Walks").setMaster("local")
    val sc = new SparkContext(conf)

    logger.info("Loading Original Graph")
    val (vertexArray, edgeArray) = loadGraph(filename)

    if (vertexArray.isEmpty || edgeArray.isEmpty) {
      logger.error("Vertex or Edge array is empty, cannot proceed with graph creation.")
      throw new IllegalStateException("Graph data arrays cannot be empty.")
    }
    val maxIteration = Math.sqrt(vertexArray.size).toInt
    logger.info(s"Max Iteration $maxIteration")

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

    val EdgeRDD: RDD[Edge[EdgeData]] = sc.parallelize(edgeArray.map(e => Edge(e.fromId, e.toId, e)))

    val graph = Graph(VertexRDD, EdgeRDD)

    def vertexProgram(id: VertexId,
                       attr: (VertexData, List[(VertexId, List[(Int, Double)])], Option[List[(Int, Double)]]),
                       msg: List[(VertexId, List[(Int, Double)])]
                     ): (VertexData, List[(VertexId, List[(Int, Double)])], Option[List[(Int, Double)]]) = {
      if (msg.nonEmpty && msg.length >= maxIteration) {
        //logger.warn(s"Vertex $id received a message list longer than expected: ${msg.length}")
      }
      if (msg.nonEmpty && msg.length < 10) {
        val matcher = attr._3.getOrElse(VertexSimilarity.calcMaxSimilarity(attr._1))
        (attr._1, msg :+ (id, matcher), Some(matcher))
      } else {
        attr
      }
    }

    def sendMessage(edge: EdgeTriplet[(VertexData, List[(VertexId, List[(Int, Double)])], Option[List[(Int, Double)]]), EdgeData]
                   ): Iterator[(VertexId, List[(VertexId, List[(Int, Double)])])] = {
      if (edge.srcAttr._2.isEmpty) {
        //logger.warn(s"Trying to send a message from vertex ${edge.srcId} which has no path information.")
      }
      if (edge.srcAttr._2.nonEmpty && edge.srcAttr._2.length < maxIteration) {
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

    val resultGraph = graph.pregel(initialMessage, 20, EdgeDirection.Out)(
      (id, attr, msg) => vertexProgram(id, attr, msg),
      sendMessage,
      messageCombiner
    )
    resultGraph.vertices.filter(_._2._2.nonEmpty).collect().foreach { case (_, (_, path, _)) =>
      val pathStr = path.flatMap { case (vid, matchers) =>
        matchers.map { case (intVal, doubleVal) => s"$vid : ($intVal, $doubleVal)" }
      }.mkString(" -> ")
    }

    val resultMap: Array[Map[VertexId, Matcher]] = resultGraph.vertices.filter(_._2._2.nonEmpty).map {
      case (_, (_, path, _)) =>
        path.flatMap { case (vid, matchers) =>
          matchers.map { case (intVal, doubleVal) =>
            (vid, (intVal, doubleVal))
          }
        }
    }.collect().map(_.toMap)

    if (resultMap.isEmpty) {
      logger.warn("The result map is empty after Pregel operation. This may indicate a problem with the algorithm or data.")
    }

    sc.stop()
    logger.info("Spark Context Stopped!")

    resultMap
  }
}
