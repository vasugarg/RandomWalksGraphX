package cs441.HW2

import GraphXUtilz.GraphLoader.{VertexData, loadGraph}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import com.typesafe.config.ConfigFactory

class VertexSimilarityTest extends AnyFunSpec with Matchers {
  describe("VertexSimilarity.calcMaxSimilarity") {

    it("should calculate maximum similarity correctly for a given vertex") {
      val testVertex = VertexData(1, 2, 3, 4, 5, 6, 7, 8, 0.5, valuableData = true)
      val result = VertexSimilarity.calcMaxSimilarity(testVertex)

      result should not be empty
      result.head should matchPattern { case (_, score: Double) if score >= 0.0 && score <= 1.0 => }
    }

    it("should return a list with one tuple containing the id of the most similar vertex and the similarity score") {
      val testVertex = VertexData(1, 2, 3, 4, 5, 6, 7, 8, 0.5, valuableData = true)
      val result = VertexSimilarity.calcMaxSimilarity(testVertex)

      result should have length 1
      result.head._1 should be >= 0 // Assuming IDs are non-negative
    }
  }
}
