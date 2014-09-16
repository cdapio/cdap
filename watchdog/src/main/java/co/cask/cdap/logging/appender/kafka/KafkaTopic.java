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

package co.cask.cdap.logging.appender.kafka;

import java.io.IOException;

/**
 * Generates Kafka topic containing schema for logging.
 */
public final class KafkaTopic {
  // Kafka topic on which log messages will get published.
  // If there is an incompatible log schema change, then the topic version needs to be updated.
  private static final String KAFKA_TOPIC = "logs.user-v1";

  /**
   * @return Kafka topic with schema.
   * @throws IOException
   */
  public static String getTopic() throws IOException {
    return KAFKA_TOPIC;
  }
}
