package cs441.HW2

import GraphXUtilz.GraphLoader.{EdgeData, VertexData}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{BooleanType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

class RandomWalkersTest extends AnyFlatSpec with Matchers with MockitoSugar {
  val vertexArray: Array[(VertexId, VertexData)] = Array(
    (1L, VertexData(1, 2, 3, 1, 50, 5, 4, 10, 25.5, valuableData = true)),
    (2L, VertexData(2, 0, 5, 1, 40, 5, 4, 10, 33.3, valuableData = false)),
    (3L, VertexData(3, 1, 7, 1, 30, 5, 4, 10, 42.0, valuableData = true)),
    (4L, VertexData(4, 3, 2, 1, 20, 5, 4, 10, 17.8, valuableData = false)),
    (5L, VertexData(5, 1, 8, 1, 10, 5, 4, 10, 88.8, valuableData = true)),
    (6L, VertexData(6, 0, 6, 1, 60, 5, 4, 10, 66.6, valuableData = false)),
    (7L, VertexData(7, 2, 1, 1, 70, 5, 4, 10, 11.1, valuableData = true)),
    (8L, VertexData(8, 3, 9, 1, 80, 5, 4, 10, 99.9, valuableData = false)),
    (9L, VertexData(9, 1, 4, 1, 90, 5, 4, 10, 77.7, valuableData = true)),
    (10L, VertexData(10, 2, 0, 1, 100, 5, 4, 10, 55.5, valuableData = false))
  )

  // Hard-coded edge data
  val edgeSeq: Seq[Edge[EdgeData]] = Seq(
    Edge(1L, 2L, EdgeData(1L, 2L, 0.1)),
    Edge(2L, 3L, EdgeData(2L, 3L, 0.2)),
    Edge(3L, 4L, EdgeData(3L, 4L, 0.3)),
    Edge(4L, 5L, EdgeData(4L, 5L, 0.4)),
    Edge(5L, 6L, EdgeData(5L, 6L, 0.5)),
    Edge(6L, 7L, EdgeData(6L, 7L, 0.6)),
    Edge(7L, 8L, EdgeData(7L, 8L, 0.7)),
    Edge(8L, 9L, EdgeData(8L, 9L, 0.8)),
    Edge(9L, 10L, EdgeData(9L, 10L, 0.9))
  )

  // Add the last edge to complete the cycle
  val lastEdge: Edge[EdgeData] = Edge(10L, 1L, EdgeData(10L, 1L, 1.0))
  val edgeArray: Array[Edge[EdgeData]] = (edgeSeq :+ lastEdge).toArray

  val startingVertices: Set[VertexId] = Set(1L, 5L)

  val conf = new SparkConf().setAppName("Test Random Walks").setMaster("local")
  val sc = new SparkContext(conf)

  val vertexRDD: RDD[(VertexId, (VertexData, List[(VertexId, List[(Int, Double)])], Option[List[(Int, Double)]]))] =
    sc.parallelize(vertexArray.map { case (vid, vdata) =>
      val initialPath: List[(VertexId, List[(Int, Double)])] = if (startingVertices.contains(vid)) {
        List((vid, VertexSimilarity.calcMaxSimilarity(vdata))) // Initialize with self and matcher value
      } else {
        List.empty[(VertexId, List[(Int, Double)])]
      }
      (vid, (vdata, initialPath, None))
    })
  val edgeRDD: RDD[Edge[EdgeData]] = sc.parallelize(edgeArray)
  val graph = Graph(vertexRDD, edgeRDD)


  "RDD operation" should "correctly convert elements to upper case" in {
    val sourceRDD: RDD[String] = sc.parallelize(Seq("jose", "li")).map(_.toUpperCase)
    val expectedRDD: RDD[String] = sc.parallelize(Seq("JOSE", "LI"))
    // Custom assertion for RDDs using ScalaTest matchers
    sourceRDD.collect() should contain theSameElementsAs expectedRDD.collect()
  }
  "Graph operation" should "filter valuable vertices and convert to DataFrame" in {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    import spark.implicits._

    // Convert the vertices to a DataFrame
    val verticesDF = graph.vertices.map {
      case (id, (vdata, _, _)) => (id, vdata.propValueRange, vdata.valuableData)
    }.toDF("id", "weight", "valuableData")

    // Apply a filter to select only the valuable vertices
    val valuableVerticesDF = verticesDF.filter($"valuableData" === true)

    // Create an expected DataFrame manually for test
    val expectedVerticesData = Seq(
      Row(1L, 50.0, true),
      Row(3L, 30.0, true),
      Row(5L, 10.0, true),
      Row(7L, 70.0, true),
      Row(9L, 90.0, true)
    )

    // Define the schema for the expected DataFrame
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("weight", DoubleType, nullable = false),
      StructField("valuableData", BooleanType, nullable = false)
    ))

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedVerticesData),
      schema
    )
    // Collect and compare the DataFrames
    valuableVerticesDF.collect() should contain theSameElementsAs expectedDF.collect()
  }
}

