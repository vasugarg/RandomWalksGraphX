package GraphXUtilz

import GraphXUtilz.GraphLoader.{EdgeData, VertexData}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


class GraphLoaderTest extends AnyFunSpec with Matchers {

  describe("GraphLoader.loadGraph") {

    it("should return list of verices and nodes when the graph exist") {
      val filepath = "src/test/resources/inputTest/NetGraph_29-10-23-21-39-22.ngs"
      val result = GraphLoader.loadGraph(filepath)

      result shouldBe a [(List[VertexData], List[EdgeData])]
    }
  }
}
