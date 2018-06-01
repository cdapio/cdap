/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.app.guice.ClusterMode;

/**
 * Defines constants used across different modules.
 */
public final class ProgramOptionConstants {

  public static final String PROGRAM_JAR = "programJar";

  public static final String EXPANDED_PROGRAM_JAR = "expandedProgramJar";

  public static final String CDAP_CONF_FILE = "cConfFile";

  public static final String HADOOP_CONF_FILE = "hConfFile";

  public static final String APP_SPEC_FILE = "appSpecFile";

  public static final String RUN_ID = "runId";

  public static final String TWILL_RUN_ID = "twillRunId";

  public static final String FLOWLET_NAME = "flowletName";

  public static final String CLUSTER_STATUS = "clusterStatus";

  public static final String CLUSTER = "cluster";

  public static final String CLUSTER_KEY_INFO = "clusterKeyInfo";

  public static final String DEBUG_ENABLED = "debugEnabled";

  public static final String PROGRAM_STATUS = "programStatus";

  public static final String PROGRAM_RUN_ID = "programRunId";

  public static final String INSTANCE_ID = "instanceId";

  public static final String INSTANCES = "instances";

  public static final String HOST = "host";

  public static final String LOGICAL_START_TIME = "logical.start.time";

  public static final String END_TIME = "endTime";

  public static final String PROGRAM_DESCRIPTOR = "programDescriptor";

  public static final String SUSPEND_TIME = "suspendTime";

  public static final String RESUME_TIME = "resumeTime";

  public static final String PROGRAM_NAME_IN_WORKFLOW = "programNameInWorkflow";

  public static final String WORKFLOW_TOKEN = "workflowToken";

  public static final String WORKFLOW_RUN_ID = "workflowRunId";

  public static final String WORKFLOW_NODE_ID = "workflowNodeId";

  public static final String WORKFLOW_NAME = "workflowName";

  public static final String SCHEDULE_ID = "scheduleId";

  public static final String SCHEDULE_NAME = "scheduleName";

  public static final String CRON_EXPRESSION = "cronExpression";

  public static final String SYSTEM_OVERRIDES = "systemOverrides";

  public static final String USER_OVERRIDES = "userOverrides";

  public static final String TRIGGERING_SCHEDULE_INFO = "triggeringScheduleInfo";

  public static final String PROGRAM_ERROR = "programError";

  public static final String USER_ID = "userId";

  public static final String SKIP_PROVISIONING = "skipProvisioning";

  /**
   * Option to a local file path of a directory containing plugins artifacts.
   */
  public static final String PLUGIN_DIR = "pluginDir";

  /**
   * Option to a local file path of a JAR file containing plugins artifacts.
   */
  public static final String PLUGIN_ARCHIVE = "pluginArchive";

  /**
   * Options for impersonation
   */
  // This is the principal that the program will be run as. Currently, it will be either the app principal if that
  // exists or it will be the namespace principal.
  public static final String PRINCIPAL = "principal";

  public static final String APP_PRINCIPAL_EXISTS = "appPrincipalExists";

  /**
   * Option for the program artifact id.
   */
  public static final String ARTIFACT_ID = "artifactId";

  /**
   * Option for the program runtime {@link ClusterMode}.
   */
  public static final String CLUSTER_MODE = "clusterMode";
}
