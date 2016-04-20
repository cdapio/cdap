package co.cask.cdap.examples.sparkstreaming

import java.util

import co.cask.cdap.api.spark.{SparkExecutionContext, SparkMain}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by rsinha on 4/19/16.
  */
class SparkKafkaWordCountProgram extends SparkMain {

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext
    val streamingContext: StreamingContext = new StreamingContext(sc, Duration(10))

    val arguments: util.Map[String, String] = sec.getRuntimeArguments

    println("The arguments are: " + arguments)
  }
}
