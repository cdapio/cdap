package com.continuuity.common.conf;

/**
 * Configuration parameters for Kafka server.
 */
public final class KafkaConstants {

  /**
   * Keys for configuration parameters.
   */
  public static final class ConfigKeys {

    public static final String PORT_CONFIG = "kafka.port";
    public static final String NUM_PARTITIONS_CONFIG = "kafka.num.partitions";
    public static final String LOG_DIR_CONFIG = "kafka.log.dir";
    public static final String HOSTNAME_CONFIG = "kafka.bind.hostname";
    public static final String ZOOKEEPER_NAMESPACE_CONFIG = "kafka.zookeeper.namespace";
    public static final String REPLICATION_FACTOR = "kafka.default.replication.factor";
  }

  public static final int DEFAULT_NUM_PARTITIONS = 1;
  public static final int DEFAULT_REPLICATION_FACTOR = 1;
}
