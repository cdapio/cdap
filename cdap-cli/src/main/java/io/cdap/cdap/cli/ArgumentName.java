/*
 * Copyright © 2012-2019 Cask Data, Inc.
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

package io.cdap.cdap.cli;

/**
 * Argument names.
 */
public enum ArgumentName {
  /**
   * Commons
   */
  DESCRIPTION("description"),

  PROGRAM("app-id.program-id"),
  WORKER("app-id.worker-id"),
  METHOD("app-id.method-id"),
  WORKFLOW("app-id.workflow-id"),
  SERVICE("app-id.service-id"),
  MAPREDUCE("app-id.mapreduce-id"),
  SPARK("app-id.spark-id"),
  SCHEDULE("app-id.schedule-id"),

  APP_CONFIG("app-config"),
  APP_CONFIG_FILE("app-config-file"),
  RUNTIME_ARGS("runtime-args"),
  PREFERENCES("preferences"),
  ROUTE_CONFIG("route-config"),
  HOSTNAME("hostname"),
  DATASET_TYPE("dataset-type"),
  DATASET_MODULE("dataset-module"),
  DATASET_PROPERTIES("dataset-properties"),
  DATASET_DESCRIPTON("dataset-description"),
  NEW_DATASET_MODULE("new-dataset-module"),
  DATASET("dataset-name"),
  NEW_DATASET("new-dataset-name"),
  CONTENT_TYPE("content-type"),
  LOCAL_FILE_PATH("local-file-path"),
  PARAMETER_MAP("parameter-map"),
  TTL_IN_SECONDS("ttl-in-seconds"),
  NUM_INSTANCES("num-instances"),
  START_TIME("start-time"),
  END_TIME("end-time"),
  TIMEOUT("timeout"),
  LIMIT("limit"),
  RUN_STATUS("status"),
  RUN_ID("runid"),
  APP_JAR_FILE("app-jar-file"),
  DATASET_MODULE_JAR_FILE("module-jar-file"),
  DATASET_MODULE_JAR_CLASSNAME("module-jar-classname"),
  QUERY("query"),
  APP("app-id"),
  APP_VERSION("app-version"),
  PROGRAM_TYPES("program-types"),
  HTTP_METHOD("http-method"),
  ENDPOINT("endpoint"),
  HEADERS("headers"),
  HTTP_BODY("body"),
  CRON_EXPRESSION("cron-expression"),
  SCHEDULE_NAME("schedule-name"),
  SCHEDULE_PROPERTIES("schedule-properties"),
  CONCURRENCY("concurrency"),

  SCHEMA("schema"),
  SETTINGS("settings"),
  FREQUENCY("frequency"),

  NAMESPACE_NAME("namespace-name"),
  NAMESPACE_GROUP_NAME("group-name"),
  NAMESPACE_KEYTAB_PATH("keytab-URI"),
  NAMESPACE_HBASE_NAMESPACE("hbase-namespace"),
  NAMESPACE_HIVE_DATABASE("hive-database"),
  NAMESPACE_ROOT_DIR("root-directory"),
  NAMESPACE_SCHEDULER_QUEUENAME("scheduler-queue-name"),

  INSTANCE("instance-id"),
  COMMAND_CATEGORY("command-category"),

  TABLE_RENDERER("table-renderer"),

  WORKFLOW_NODE("workflow-node"),
  WORKFLOW_TOKEN_SCOPE("workflow-token-scope"),
  WORKFLOW_TOKEN_KEY("workflow-token-key"),

  /**
   * Artifacts
   */
  ARTIFACT_NAME("artifact-name"),
  ARTIFACT_VERSION("artifact-version"),
  ARTIFACT_PARENTS("artifact-parents"),
  ARTIFACT_CONFIG_FILE("artifact-config"),
  SCOPE("scope"),
  PLUGIN_TYPE("plugin-type"),
  PLUGIN_NAME("plugin-name"),

  INSTANCE_URI("cdap-instance-uri"),
  VERIFY_SSL_CERT("verify-ssl-cert"),
  ENTITY("entity"),

  /**
   * Metadata
   */
  METADATA_SCOPE("scope"),
  SEARCH_QUERY("search-query"),
  TARGET_TYPE("target-type"),

  /**
   * Authorization
   */
  PRINCIPAL_TYPE("principal-type"),
  PRINCIPAL_NAME("principal-name"),
  ROLE_NAME("role-name"),
  PRINCIPAL("principal");

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

  public static final String ENTITY_DESCRIPTION_TEMPLATE_STRING = "<%s> " +
    "is of the form <entity-type>:<entity-id>, where <entity-type> is one of " +
    "%s" +
    "'artifact', 'application', 'dataset' or 'program'.\n" +
    "\n" +
    "%s" +
    "For artifacts and apps, " +
    "<entity-id> is composed of the namespace, entity name, and version, such as " +
    "<namespace-name>.<artifact-name>.<artifact-version> or " +
    "<namespace-name>.<app-name>.<app-version>.\n" +
    "\n" +
    "Note:  Metadata for versioned entities is not versioned, including entities such as applications, " +
    "programs, schedules, and program runs. Additions to metadata in one version are reflected in all versions.\n" +
    "\n" +
    "For programs, <entity-id> includes the " +
    "application name and the program type: " +
    "<namespace-name>.<app-name>.<program-type>.<program-name>. <program-type> is one of " +
    "mapreduce, service, spark, worker, or workflow.\n" +
    "\n" +
    "For datasets, " +
    "<entity-id> is the namespace and entity names, such as <namespace-name>.<dataset-name>.\n" +
    "\n" +
    "Custom entities can be specified as hierarchical key-value pair with an optional type if the last key in " +
    "hierarchy is not the type of the entity. For example a 'field' in dataset can be specified as: " +
    "namespace=<namespace-name>,dataset=<dataset-name>,field=<field-name>." +
    "\n" +
    "A 'jar' in a namespace can be specified as: " +
    "namespace=<namespace-name>,jar=<jar-name>,version=<version-number>,type=jar.";

  public static final String ENTITY_DESCRIPTION_STRING = String.format(ENTITY_DESCRIPTION_TEMPLATE_STRING,
    ENTITY, "", "");

  public static final String ENTITY_DESCRIPTION_ALL_STRING = String.format(ENTITY_DESCRIPTION_TEMPLATE_STRING,
    ENTITY, "'namespace', ", "For namespaces, '<entity-id>' is composed from the namespace, such as " +
    "'namespace:<namespace-name>'.\n" +
    "\n");

  public static final String ENTITY_DESCRIPTION_PERMISSIONS = "'<permissions>' is a comma-separated list of " +
    "privileges, any of 'GET', 'CREATE', 'UPDATE', 'DELETE', 'LIST', 'APPLICATION.EXECUTE', 'APPLICATION.PREVIEW', " +
    "'ACCESS.SET_OWNER' or 'ACCESS.IMPERSONATE'";
}

