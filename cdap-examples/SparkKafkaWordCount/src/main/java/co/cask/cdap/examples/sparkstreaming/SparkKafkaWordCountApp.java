package co.cask.cdap.examples.sparkstreaming;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.spark.AbstractSpark;

/**
 * Created by rsinha on 4/19/16.
 */
public class SparkKafkaWordCountApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("SparkKafkaWordCountApp");
    setDescription("Spark Streaming Example");

    // Run a Spark program on the acquired data
    addSpark(new SparkKafkaWordCountSpecification());
  }

  /**
   * A Spark Program that uses KMeans algorithm.
   */
  public static final class SparkKafkaWordCountSpecification extends AbstractSpark {

    @Override
    public void configure() {
      setName("SparkKafkaWordCountProgram");
      setDescription("Spark Streaming Kafka Word Count Program");
      setMainClass(SparkKafkaWordCountProgram.class);
    }
  }
}