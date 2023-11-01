package cs441.HW2

import Utilz.{CreateLogger, Nodes, yamlParser}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.graphx.VertexId
import org.slf4j.Logger

import scala.collection.mutable

object AttackHandler {
  val config: Config = ConfigFactory.load("application.conf")
  val logger: Logger = CreateLogger(classOf[Main.type])
  type Matcher = (Int, Double)

  // Define your thresholds and counters
  private val threshold = 0.7
  private var successfulAttackCounter = 0
  private var failedAttackCounter = 0

  private val attackedVertices = mutable.Set[VertexId]()  // To store already attacked vertices

  private def attack(node: VertexId, nodesData: Nodes): Unit = {
    if (nodesData.Modified.contains(node.toInt) || nodesData.Removed.contains(node.toInt) || nodesData.Added.contains(node.toInt)) {
      failedAttackCounter += 1
    } else {
      successfulAttackCounter += 1
    }
  }

  def handleAttacks(resultMap: Array[Map[VertexId, Matcher]], yamlFilename: String): Unit = {
    val (nodesData, _) = yamlParser.parseYaml(yamlFilename)

    resultMap.foreach { randomWalk =>
      // Filter nodes with score > threshold
      val nodesToAttack = randomWalk.filter {
        case (_, (_, score)) => score > threshold
      }.keys

      nodesToAttack.foreach { node =>
        if (!attackedVertices.contains(node)) {
          attack(node, nodesData)
          attackedVertices.add(node)
        }
      }
    }
    // Print or return your counters here if you need to
    logger.info(s"Successful Attacks: $successfulAttackCounter")
    logger.info(s"Failed Attacks: $failedAttackCounter")
  }
}

