package GraphXUtilz

import NetGraphAlgebraDefs.{Action, NetGraphComponent, NodeObject}

import java.io.{FileInputStream, InputStream, ObjectInputStream}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

import java.net.URI

trait GraphLoaderTrait {
  def loadGraph(filepath: String): (List[GraphLoader.VertexData], List[GraphLoader.EdgeData])
}

object GraphLoader extends GraphLoaderTrait {
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
                         valuableData: Boolean
                       ) extends Serializable

  @SerialVersionUID(100L)
  case class EdgeData(fromId: Long, toId: Long, cost: Double) extends Serializable

  def main(args: Array[String]): Unit = {
    val (vertexArray, edgeArray) = loadGraph("src/main/resources/input/NetGraph_29-10-23-04-33-48.ngs")
    val valuableNodes = vertexArray.filter(_.valuableData)
  }
  override def loadGraph(filepath: String): (List[VertexData], List[EdgeData]) = {

    val inputStream: InputStream = if (filepath.startsWith("s3://")) {
      val uri = new URI(filepath)
      val bucket = uri.getHost
      val key = uri.getPath.substring(1) // Remove the leading slash

      val s3Client = S3Client.builder()
        .region(Region.US_EAST_1) // Set your region
        .build()

      val getObjectRequest = GetObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()

      s3Client.getObjectAsBytes(getObjectRequest).asInputStream()
    } else {
      new FileInputStream(filepath)
    }
    val objectInputStream = new ObjectInputStream(inputStream)
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
    //logger.info("Graph Loaded!!")
    (nodes, edges)
  }
}