/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.conf;

/**
 * Configuration parameters for Kafka server.
 */
public final class KafkaConstants {

  /**
   * Keys for configuration parameters.
   */
  public static final class ConfigKeys {
    public static final String NUM_PARTITIONS_CONFIG = "kafka.server.num.partitions";
    @Deprecated
    public static final String NUM_PARTITIONS_CONFIG_DEPRECATED = "kafka.num.partitions";
    public static final String PORT_CONFIG = "kafka.server.port";
    @Deprecated
    public static final String PORT_CONFIG_DEPRECATED = "kafka.bind.port";
    public static final String HOSTNAME_CONFIG = "kafka.server.host.name";
    @Deprecated
    public static final String HOSTNAME_CONFIG_DEPRECATED = "kafka.bind.address";
    public static final String ZOOKEEPER_NAMESPACE_CONFIG = "kafka.zookeeper.namespace";
    public static final String ZOOKEEPER_QUORUM = "kafka.zookeeper.quorum";
    public static final String REPLICATION_FACTOR = "kafka.server.default.replication.factor";
    @Deprecated
    public static final String REPLICATION_FACTOR_DEPRECATED = "kafka.default.replication.factor";
    public static final String LOG_DIRS = "kafka.server.log.dirs";
    @Deprecated
    public static final String LOG_DIRS_DEPRECATED = "kafka.log.dir";
  }
}
