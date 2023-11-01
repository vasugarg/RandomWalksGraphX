package GraphXUtilz

import NetGraphAlgebraDefs.{Action, NetGraphComponent, NodeObject}
import org.apache.spark.graphx.VertexId

import java.io.{FileInputStream, ObjectInputStream}

object GraphLoader {
  @SerialVersionUID(100L)
  case class VertexData(
                         id: Long,
                         children: Int,
                         props: Int,
                         currentDepth: Int = 1,
                         propValueRange: Int,
                         maxDepth: Int,
                         maxBranchingFactor: Int,
                         maxProperties: Int,
                         storedValue: Double,
                         valuableData: Boolean,
                         //paths: List[List[VertexId]] = List(List()),
                         //visited: List[VertexId] = List(),
                         //matcher: Option[Int] = None
                       ) extends Serializable

  @SerialVersionUID(100L)
  case class EdgeData(fromId: Long, toId: Long, cost: Double) extends Serializable

  def main(args: Array[String]): Unit = {
    val (vertexArray, edgeArray) = loadGraph("outputs/NetGraph_29-10-23-21-39-22.ngs")
    val valuableNodes = vertexArray.filter(_.valuableData)
    println(valuableNodes)
    //println(nodes.filter(_.valuableData))
  }
  def loadGraph(filepath: String): (List[VertexData], List[EdgeData]) = {
    val fileInputStream = new FileInputStream(filepath)
    val objectInputStream = new ObjectInputStream(fileInputStream)
    val ng = objectInputStream.readObject().asInstanceOf[List[NetGraphComponent]]

    val nodes = ng.collect { case node: NodeObject => node }
      .map(node => VertexData(node.id.toLong,
        node.children, node.props,
        node.currentDepth,
        node.propValueRange,
        node.maxDepth,
        node.maxBranchingFactor,
        node.maxProperties,
        node.storedValue,
        node.valuableData))

    val edges = ng.collect { case edge: Action => edge }
      .map(edge => EdgeData(edge.fromNode.id, edge.toNode.id, edge.cost))
    (nodes, edges)
  }
}