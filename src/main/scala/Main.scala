package cs441.HW2
import com.typesafe.config.{Config, ConfigFactory}

object Main {

  val config: Config = ConfigFactory.load("application.conf")
  def main(args: Array[String]): Unit = {
    val results = RandomWalkers.run(config.getString("RandomWalksGraphX.FilePathsLocal.perturbedGraphPath"))
    AttackHandler.handleAttacks(results, config.getString("RandomWalksGraphX.FilePathsLocal.yamlPath"))
  }
}

