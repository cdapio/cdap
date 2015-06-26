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

package co.cask.cdap.guides.kafka;

/**
 * Contains constants used in the Application.
 */
public final class Constants {
  public static final String FLOW_NAME = "KafkaIngestionFlow";
  public static final String SERVICE_NAME = "KafkaStatsService";
  public static final String STATS_TABLE_NAME = "kafkaCounter";
  public static final String OFFSET_TABLE_NAME = "kafkaOffsets";
  public static final String KAFKA_FLOWLET = "kafkaConsumer";
  public static final String COUNTER_FLOWLET = "counterFlowlet";
  public static final String COUNT_KEY = "totalCount";
  public static final String SIZE_KEY = "totalSize";

  private Constants () {
  }
}
