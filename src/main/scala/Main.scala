package cs441.HW2
import Utilz.CreateLogger
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.Logger

object Main {
  val config: Config = ConfigFactory.load("application.conf")
  val logger: Logger = CreateLogger(classOf[Main.type])

  def main(args: Array[String]): Unit = {
    val results = RandomWalkers.run(config.getString("RandomWalksGraphX.FilePaths.perturbedGraphFilePath"))
    AttackHandler.handleAttacks(results, config.getString("RandomWalksGraphX.FilePaths.yamlPath"))
  }
}

