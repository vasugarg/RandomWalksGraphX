package Utilz

import org.slf4j.{Logger, LoggerFactory}
import scala.util.{Failure, Success, Try}

object CreateLogger {
  def apply[T](class4Logger: Class[T]): Logger = {
    val LOGBACKXML = "test.xml"
    val logger = LoggerFactory.getLogger(class4Logger.getName)
    
    Try(Option(class4Logger.getClassLoader.getResourceAsStream(LOGBACKXML))) match {
      case Failure(exception) =>
        logger.error(s"Failed to locate $LOGBACKXML for reason $exception")

      case Success(maybeStream) =>
        maybeStream.foreach(_.close()) // Safer way to close the stream if it exists.
    }

    logger
  }
}

