package cs441.HW2

import GraphXUtilz.GraphLoader.{VertexData, loadGraph}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, LogManager}


object VertexSimilarity {
  val logger = LogManager.getRootLogger
  logger.setLevel(Level.INFO)
  logger.setLevel(Level.WARN)
  logger.setLevel(Level.DEBUG)
  logger.setLevel(Level.ERROR)
  val config: Config = ConfigFactory.load("application.conf")

  logger.info("Loading the Original Graph")
  val (vertexArray, edgeArray) = loadGraph(config.getString("RandomWalksGraphX.FilePathsLocal.originalGraphPath")) // Loads the original Graph
  val valuableNodes: Seq[VertexData] = vertexArray.filter(_.valuableData)

  def calcMaxSimilarity(vertex: VertexData): List[(Int, Double)] = {
    val setProps = Set(vertex.children, vertex.props, vertex.currentDepth, vertex.propValueRange,
      vertex.maxDepth, vertex.maxBranchingFactor, vertex.maxProperties, vertex.storedValue)

    // Use foldLeft to iterate over the nodes and find the maximum score
    val (maxNodeId, maxScore) = valuableNodes.foldLeft((-1, 0.0)) { case ((currentMaxId, currentMaxScore), node) =>
      val setPropsVal = Set(node.children, node.props, node.currentDepth, node.propValueRange,
        node.maxDepth, node.maxBranchingFactor, node.maxProperties, node.storedValue)
      val intersectionSize = setProps.intersect(setPropsVal).size.toDouble
      val unionSize = setProps.union(setPropsVal).size.toDouble
      val score = if (unionSize == 0.0) 0.0 else intersectionSize / unionSize
      val roundedScore = (score * 1000).round / 1000.toDouble

      if (roundedScore > currentMaxScore) {
        (node.id.toInt, roundedScore)
      } else {
        (currentMaxId, currentMaxScore)
      }
    }
    List((maxNodeId, maxScore))
  }

  def main(args: Array[String]): Unit = {
    val v = VertexData(2,3,6,1,75,1,6,12,0.7340689500727868,valuableData = true)
    println(calcMaxSimilarity(v))
  }
}
