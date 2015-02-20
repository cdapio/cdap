/*
 * Copyright © 2012-2015 Cask Data, Inc.
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

package co.cask.cdap.cli;

/**
 * Argument names.
 */
public enum ArgumentName {
  PROGRAM("app-id.program-id"),
  STREAM("stream-id"),
  PROCEDURE("app-id.procedure-id"),
  METHOD("app-id.method-id"),
  FLOW("app-id.flow-id"),
  FLOWLET("app-id.flow-id.flowlet-id"),
  WORKFLOW("app-id.workflow-id"),
  SERVICE("app-id.service-id"),
  RUNNABLE("app-id.service-id.runnable-id"),
  MAPREDUCE("app-id.mapreduce-id"),
  SPARK("app-id.spark-id"),

  RUNTIME_ARGS("runtime-args"),
  HOSTNAME("hostname"),
  DATASET_TYPE("dataset-type"),
  DATASET_MODULE("dataset-module"),
  DATASET_PROPERTIES("dataset-properties"),
  NEW_DATASET_MODULE("new-dataset-module"),
  DATASET("dataset-name"),
  NEW_DATASET("new-dataset-name"),
  STREAM_EVENT("stream-event"),
  CONTENT_TYPE("content-type"),
  LOCAL_FILE_PATH("local-file-path"),
  NEW_STREAM("new-stream-id"),
  PARAMETER_MAP("parameter-map"),
  TTL_IN_SECONDS("ttl-in-seconds"),
  NUM_INSTANCES("num-instances"),
  START_TIME("start-time"),
  END_TIME("end-time"),
  TIMEOUT("timeout"),
  LIMIT("limit"),
  RUN_STATUS("status"),
  APP_JAR_FILE("app-jar-file"),
  DATASET_MODULE_JAR_FILE("module-jar-file"),
  DATASET_MODULE_JAR_CLASSNAME("module-jar-classname"),
  QUERY("query"),
  APP("app-id"),
  HTTP_METHOD("http-method"),
  ENDPOINT("endpoint"),
  HEADERS("headers"),
  HTTP_BODY("body"),
  FORMAT("format"),
  SCHEMA("schema"),
  SETTINGS("settings"),
  FREQUENCY("frequency"),

  NAMESPACE_ID("namespace-id"),
  NAMESPACE_DISPLAY_NAME("namespace-display-name"),
  NAMESPACE_DESCRIPTION("namespace-description"),

  INSTANCE("instance-id"),
  ADAPTER_TYPE("adapter-type"),
  ADAPTER_PROPS("adapter-props"),
  ADAPTER_SOURCE("adapter-source"),
  ADAPTER_SOURCE_PROPS("adapter-source-config"),
  ADAPTER_SINK("adapter-sink"),
  ADAPTER_SINK_PROPS("adapter-sink-config"),
  ADAPTER("adapter-name"),
  COMMAND_CATEGORY("command-category");

  private final String name;

  ArgumentName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return name;
  }
}
