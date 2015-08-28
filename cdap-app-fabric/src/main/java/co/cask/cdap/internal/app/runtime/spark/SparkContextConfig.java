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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.proto.Id;
import co.cask.tephra.Transaction;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.RunId;

import java.lang.reflect.Type;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Helper class for getting and setting specific config settings for a spark job context.
 */
public class SparkContextConfig {

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Type ARGS_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  /**
   * Configuration key for boolean value to tell whether Spark program is executed on a cluster or not.
   */
  public static final String HCONF_ATTR_CLUSTER_MODE = "cdap.spark.cluster.mode";

  private static final String HCONF_ATTR_APP_SPEC = "cdap.spark.app.spec";
  private static final String HCONF_ATTR_PROGRAM_SPEC = "cdap.spark.program.spec";
  private static final String HCONF_ATTR_PROGRAM_ID = "cdap.spark.program.id";
  private static final String HCONF_ATTR_RUN_ID = "cdap.spark.run.id";
  private static final String HCONF_ATTR_LOGICAL_START_TIME = "hconf.program.logical.start.time";
  private static final String HCONF_ATTR_ARGS = "hconf.program.args";
  private static final String HCONF_ATTR_NEW_TX = "hconf.program.newtx.tx";
  private static final String HCONF_ATTR_WORKFLOW_TOKEN = "hconf.program.workflow.token";

  private final Configuration hConf;

  /**
   * Creates an instance by copying from the given configuration.
   */
  public SparkContextConfig(Configuration hConf) {
    this.hConf = new Configuration(hConf);
  }

  /**
   * Returns the configuration.
   */
  public Configuration getConfiguration() {
    return hConf;
  }

  /**
   * Returns true if in local mode.
   */
  public boolean isLocal() {
    return !hConf.getBoolean(HCONF_ATTR_CLUSTER_MODE, false);
  }

  /**
   * Sets configurations based on the given context.
   */
  public SparkContextConfig set(ExecutionSparkContext context) {
    setApplicationSpecification(context.getApplicationSpecification());
    setSpecification(context.getSpecification());
    setProgramId(context.getProgramId());
    setRunId(context.getRunId().getId());
    setLogicalStartTime(context.getLogicalStartTime());
    setArguments(context.getRuntimeArguments());
    setTransaction(context.getTransaction());
    setWorkflowToken(context.getWorkflowToken());

    return this;
  }

  public ApplicationSpecification getApplicationSpecification() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_APP_SPEC), ApplicationSpecification.class);
  }

  /**
   * @return the {@link SparkSpecification} stored in the configuration.
   */
  public SparkSpecification getSpecification() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_PROGRAM_SPEC), SparkSpecification.class);
  }

  /**
   * @return the {@link Id.Program} stored in the configuration.
   */
  public Id.Program getProgramId() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_PROGRAM_ID), Id.Program.class);
  }

  /**
   * @return the {@link RunId} stored in the configuration.
   */
  public RunId getRunId() {
    return RunIds.fromString(hConf.get(HCONF_ATTR_RUN_ID));
  }

  /**
   * @return the runtime arguments stored in the configuration.
   */
  public Map<String, String> getArguments() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_ARGS), ARGS_TYPE);
  }

  /**
   * @return the logical start time stored in the configuration.
   */
  public long getLogicalStartTime() {
    return hConf.getLong(HCONF_ATTR_LOGICAL_START_TIME, System.currentTimeMillis());
  }

  /**
   * @return the {@link Transaction} stored in the configuration.
   */
  public Transaction getTransaction() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_NEW_TX), Transaction.class);
  }

  /**
   * @return the {@link WorkflowToken} stored in the configuration.
   */
  @Nullable
  public WorkflowToken getWorkflowToken() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_WORKFLOW_TOKEN), BasicWorkflowToken.class);
  }

  private void setApplicationSpecification(ApplicationSpecification spec) {
    hConf.set(HCONF_ATTR_APP_SPEC, GSON.toJson(spec));
  }

  private void setSpecification(SparkSpecification spec) {
    hConf.set(HCONF_ATTR_PROGRAM_SPEC, GSON.toJson(spec));
  }

  private void setProgramId(Id.Program programId) {
    hConf.set(HCONF_ATTR_PROGRAM_ID, GSON.toJson(programId));
  }

  private void setRunId(String runId) {
    hConf.set(HCONF_ATTR_RUN_ID, runId);
  }

  private void setArguments(Map<String, String> runtimeArgs) {
    hConf.set(HCONF_ATTR_ARGS, GSON.toJson(runtimeArgs, ARGS_TYPE));
  }

  private void setLogicalStartTime(long startTime) {
    hConf.setLong(HCONF_ATTR_LOGICAL_START_TIME, startTime);
  }

  private void setTransaction(Transaction tx) {
    hConf.set(HCONF_ATTR_NEW_TX, GSON.toJson(tx));
  }

  public void setWorkflowToken(@Nullable WorkflowToken workflowToken) {
    hConf.set(HCONF_ATTR_WORKFLOW_TOKEN, GSON.toJson(workflowToken));
  }
}
