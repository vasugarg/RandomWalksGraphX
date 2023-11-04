package Utilz

//import org.slf4j.Logger
import org.yaml.snakeyaml.Yaml
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

import java.io.ByteArrayOutputStream
import java.util.{List => JList, Map => JMap}
import scala.io.Source
import scala.jdk.CollectionConverters._


case class Nodes(
                  Modified: Map[Int, Int],
                  Removed: Map[Int, Int],
                  Added: Map[Int, Int]
                )

case class Edges(
                  Modified: Map[Int, Int],
                  Added: Map[Int, Int],
                  Removed: Map[Int, Int]
                )

object yamlParser {
  //val logger: Logger = CreateLogger(this.getClass)

  def parseYaml(filename: String): (Nodes, Edges) = {
    //logger.info("Started parsing the YAML file")

    if (filename.startsWith("s3:")) {
      val s3UriParts = filename.stripPrefix("s3://").split("/")
      val bucket = s3UriParts.head
      val objectKey = s3UriParts.tail.mkString("/")
      val awsRegion = Region.US_EAST_1
      val s3 = S3Client.builder().region(awsRegion).build()

      // Fetch the YAML file from S3
      val getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(objectKey).build()
      val response = s3.getObject(getObjectRequest)

      try {
        val byteArrayOutputStream = new ByteArrayOutputStream()
        val buffer = new Array[Byte](1024)
        var bytesRead = 0

        while ( {
          bytesRead = response.read(buffer)
          bytesRead != -1
        }) {
          byteArrayOutputStream.write(buffer, 0, bytesRead)
        }

        val yamlBytes = byteArrayOutputStream.toByteArray()
        val yamlStr = new String(yamlBytes, "UTF-8").replaceAll("\t", "    ")

        val yaml = new Yaml()
        //logger.info("Loading the YAML data to JSON")
        val data = yaml.load(yamlStr).asInstanceOf[JMap[String, JMap[String, Any]]]

        val nodesData = data.get("Nodes").asScala.toMap
        val edgesData = data.get("Edges").asScala.toMap

        val nodes = Nodes(
          nodesData("Modified").asInstanceOf[JList[Int]].asScala.map(x => (x, x)).toMap,
          nodesData("Removed").asInstanceOf[JList[Int]].asScala.map(x => (x, x)).toMap,
          nodesData("Added").asInstanceOf[JMap[Int, Int]].asScala.toMap
        )

        val edges = Edges(
          edgesData("Modified").asInstanceOf[JMap[Int, Int]].asScala.toMap,
          edgesData("Added").asInstanceOf[JMap[Int, Int]].asScala.toMap,
          edgesData("Removed").asInstanceOf[JMap[Int, Int]].asScala.toMap
        )
        (nodes, edges)
      } finally {
        response.close()
        s3.close()
      }
    } else {
      // Read the local file
      val yamlStr = Source.fromFile(filename).mkString.replaceAll("\t", "    ")

      val yaml = new Yaml()
      //logger.info("Loading the YAML data to JSON")
      val data = yaml.load(yamlStr).asInstanceOf[JMap[String, JMap[String, Any]]]

      val nodesData = data.get("Nodes").asScala.toMap
      val edgesData = data.get("Edges").asScala.toMap

      val nodes = Nodes(
        nodesData("Modified").asInstanceOf[JList[Int]].asScala.map(x => (x, x)).toMap,
        nodesData("Removed").asInstanceOf[JList[Int]].asScala.map(x => (x, x)).toMap,
        nodesData("Added").asInstanceOf[JMap[Int, Int]].asScala.toMap
      )

      val edges = Edges(
        edgesData("Modified").asInstanceOf[JMap[Int, Int]].asScala.toMap,
        edgesData("Added").asInstanceOf[JMap[Int, Int]].asScala.toMap,
        edgesData("Removed").asInstanceOf[JMap[Int, Int]].asScala.toMap
      )
      (nodes, edges)
    }
  }
}
