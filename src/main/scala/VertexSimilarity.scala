package cs441.HW2

import GraphXUtilz.GraphLoader.{VertexData, loadGraph}
import Utilz.CreateLogger
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.Logger

object VertexSimilarity {
  val config: Config = ConfigFactory.load("application.conf")
  val logger: Logger = CreateLogger(classOf[Main.type])

  val (vertexArray, edgeArray) = loadGraph(config.getString("RandomWalksGraphX.FilePaths.originalGraphFilePath")) // Loads the original Graph
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
