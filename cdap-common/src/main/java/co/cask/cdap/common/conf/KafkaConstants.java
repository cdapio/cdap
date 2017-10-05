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
    public static final String ZOOKEEPER_NAMESPACE_CONFIG = "kafka.zookeeper.namespace";
    public static final String ZOOKEEPER_QUORUM = "kafka.zookeeper.quorum";
  }
}
