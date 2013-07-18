package com.continuuity.kafka.run;

/**
 * Configuration parameters for Kafka server.
 */
public final class KafkaConstants {

  /**
   * Keys for configuration parameters.
   */
  public static final class ConfigKeys {

    public static final String KAFKA_PORT_CONFIG = "kafka.port";
    public static final String KAFKA_NUM_PARTITIONS_CONFIG = "kafka.num.partitions";
    public static final String KAFKA_LOG_DIR_CONFIG = "kafka.log.dir";
    public static final String KAFKA_HOSTNAME_CONFIG = "kafka.bind.hostname";
    public static final String KAFKA_ZOOKEEPER_NAMESPACE_CONFIG = "kafka.zookeeper.namespace";
  }

  public static final int DEFAULT_NUM_PARTITIONS = 1;
}
