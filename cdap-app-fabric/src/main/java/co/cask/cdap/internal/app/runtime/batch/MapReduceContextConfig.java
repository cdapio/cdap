/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.data.batch.SimpleSplit;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetInputFormat;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetOutputFormat;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.tephra.Transaction;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Helper class for getting and setting specific config settings for a job context.
 */
public final class MapReduceContextConfig {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceContextConfig.class);
  private static final Gson GSON = new Gson();

  private static final String HCONF_ATTR_RUN_ID = "hconf.program.run.id";
  private static final String HCONF_ATTR_LOGICAL_START_TIME = "hconf.program.logical.start.time";
  private static final String HCONF_ATTR_PROGRAM_NAME_IN_WORKFLOW = "hconf.program.name.in.workflow";
  private static final String HCONF_ATTR_WORKFLOW_TOKEN = "hconf.program.workflow.token";
  private static final String HCONF_ATTR_PLUGINS = "hconf.program.plugins.map";
  private static final String HCONF_ATTR_ARGS = "hconf.program.args";
  private static final String HCONF_ATTR_PROGRAM_JAR_URI = "hconf.program.jar.uri";
  private static final String HCONF_ATTR_CCONF = "hconf.cconf";
  private static final String HCONF_ATTR_INPUT_SPLIT_CLASS = "hconf.program.input.split.class";
  private static final String HCONF_ATTR_INPUT_SPLITS = "hconf.program.input.splits";
  private static final String HCONF_ATTR_NEW_TX = "hconf.program.newtx.tx";

  private final Configuration hConf;

  public MapReduceContextConfig(Configuration hConf) {
    this.hConf = hConf;
  }

  public Configuration getHConf() {
    return hConf;
  }

  /**
   * Updates the {@link Configuration} of this class with the given paramters.
   *
   * @param context the context for the MapReduce program
   * @param conf the CDAP configuration
   * @param tx the long transaction created for the MapReduce program
   * @param programJarURI The URI of the program JAR
   */
  public void set(BasicMapReduceContext context, CConfiguration conf, Transaction tx, URI programJarURI) {
    setRunId(context.getRunId().getId());
    setLogicalStartTime(context.getLogicalStartTime());
    setProgramNameInWorkflow(context.getProgramNameInWorkflow());
    setWorkflowToken(context.getWorkflowToken());
    setPlugins(context.getPlugins());
    setArguments(context.getRuntimeArguments());
    setProgramJarURI(programJarURI);
    setConf(conf);
    setTx(tx);
    setInputSelection(context.getInputDataSelection());
  }

  private void setArguments(Map<String, String> arguments) {
    hConf.set(HCONF_ATTR_ARGS, GSON.toJson(arguments));
  }

  /**
   * Returns the runtime arguments for the MapReduce program
   */
  public Arguments getArguments() {
    Map<String, String> arguments = GSON.fromJson(hConf.get(HCONF_ATTR_ARGS),
                                                  new TypeToken<Map<String, String>>() {
                                                  }.getType());
    return new BasicArguments(arguments);
  }

  private void setRunId(String runId) {
    hConf.set(HCONF_ATTR_RUN_ID, runId);
  }

  /**
   * Returns the {@link RunId} for the MapReduce program.
   */
  public RunId getRunId() {
    return RunIds.fromString(hConf.get(HCONF_ATTR_RUN_ID));
  }

  private void setLogicalStartTime(long startTime) {
    hConf.setLong(HCONF_ATTR_LOGICAL_START_TIME, startTime);
  }

  /**
   * Returns the logical start time for the MapReduce program.
   */
  public long getLogicalStartTime() {
    long startTime = hConf.getLong(HCONF_ATTR_LOGICAL_START_TIME, -1L);
    // It shouldn't happen
    Preconditions.checkArgument(startTime >= 0, "Logical start time is not present.");
    return startTime;
  }

  private void setProgramNameInWorkflow(@Nullable String programNameInWorkflow) {
    if (programNameInWorkflow != null) {
      hConf.set(HCONF_ATTR_PROGRAM_NAME_IN_WORKFLOW, programNameInWorkflow);
    }
  }

  /**
   * Returns the MapReduce program name when it is running inside Workflow or {@code null} if not.
   */
  @Nullable
  public String getProgramNameInWorkflow() {
    return hConf.get(HCONF_ATTR_PROGRAM_NAME_IN_WORKFLOW);
  }

  private void setWorkflowToken(@Nullable WorkflowToken workflowToken) {
    if (workflowToken != null) {
      hConf.set(HCONF_ATTR_WORKFLOW_TOKEN, GSON.toJson(workflowToken));
    }
  }

  /**
   * Returns the {@link WorkflowToken} if it is running inside Workflow or {@code null} if not.
   */
  @Nullable
  public WorkflowToken getWorkflowToken() {
    String tokenJson = hConf.get(HCONF_ATTR_WORKFLOW_TOKEN);
    if (tokenJson == null) {
      return null;
    }
    BasicWorkflowToken token = GSON.fromJson(tokenJson, BasicWorkflowToken.class);
    token.disablePut();
    return token;
  }

  private void setPlugins(Map<String, Plugin> plugins) {
    hConf.set(HCONF_ATTR_PLUGINS, GSON.toJson(plugins));
  }

  /**
   * Returns the plugins being used in the MapReduce program.
   */
  public Map<String, Plugin> getPlugins() {
    String spec = hConf.get(HCONF_ATTR_PLUGINS);
    if (spec == null) {
      return ImmutableMap.of();
    }
    return GSON.fromJson(spec, new TypeToken<Map<String, Plugin>>() { }.getType());
  }

  private void setProgramJarURI(URI programJarURI) {
    hConf.set(HCONF_ATTR_PROGRAM_JAR_URI, programJarURI.toASCIIString());
  }

  /**
   * Returns the URI of where the program JAR is.
   */
  public URI getProgramJarURI() {
    return URI.create(hConf.get(HCONF_ATTR_PROGRAM_JAR_URI));
  }

  /**
   * Returns the file name of the program JAR.
   */
  public String getProgramJarName() {
    return new Path(getProgramJarURI()).getName();
  }

  /**
   * Returns the name of the Dataset that is used as input for the MapReduce program or {@code null} if
   * Dataset is not used for input.
   */
  @Nullable
  public String getInputDataSet() {
    return hConf.get(DataSetInputFormat.HCONF_ATTR_INPUT_DATASET);
  }

  private void setInputSelection(@Nullable List<Split> splits) {
    if (splits == null) {
      return;
    }
    // todo: this is ugly
    Class<? extends Split> splitClass;
    if (splits.size() > 0) {
      splitClass = splits.get(0).getClass();
    } else {
      // assign any
      splitClass = SimpleSplit.class;
    }
    hConf.set(HCONF_ATTR_INPUT_SPLIT_CLASS, splitClass.getName());
    hConf.set(HCONF_ATTR_INPUT_SPLITS, GSON.toJson(splits));
  }

  /**
   * Returns the list of {@link Split} objects for the MapReduce program.
   */
  public List<Split> getInputSelection() {
    String splitClassName = hConf.get(HCONF_ATTR_INPUT_SPLIT_CLASS);
    String splitsJson = hConf.get(HCONF_ATTR_INPUT_SPLITS);
    if (splitClassName == null || splitsJson == null) {
      return Collections.emptyList();
    }

    try {
      // Yes, we know that it implements Split
      @SuppressWarnings("unchecked")
      Class<? extends Split> splitClass =
        (Class<? extends Split>) hConf.getClassLoader().loadClass(splitClassName);
      return GSON.fromJson(splitsJson, new ListSplitType(splitClass));
    } catch (ClassNotFoundException e) {
      //todo
      throw Throwables.propagate(e);
    }
  }

  // This is needed to deserialize JSON into generified List
  private static final class ListSplitType implements ParameterizedType {
    private final Class<? extends Split> implementationClass;

    private ListSplitType(Class<? extends Split> implementationClass) {
      this.implementationClass = implementationClass;
    }

    @Override
    public Type[] getActualTypeArguments() {
      return new Type[]{implementationClass};
    }

    @Override
    public Type getRawType() {
      return List.class;
    }

    @Override
    public Type getOwnerType() {
      // it is fine, as it is not inner class
      return null;
    }
  }

  /**
   * Returns the name of the Dataset that is used as output for the MapReduce program or {@code null} if
   * Dataset is not used for output.
   */
  @Nullable
  public String getOutputDataSet() {
    return hConf.get(DataSetOutputFormat.HCONF_ATTR_OUTPUT_DATASET);
  }

  private void setConf(CConfiguration conf) {
    StringWriter stringWriter = new StringWriter();
    try {
      conf.writeXml(stringWriter);
    } catch (IOException e) {
      LOG.error("Unable to serialize CConfiguration into xml");
      throw Throwables.propagate(e);
    }
    hConf.set(HCONF_ATTR_CCONF, stringWriter.toString());
  }

  /**
   * Returns the {@link CConfiguration} stored inside the job {@link Configuration}.
   */
  public CConfiguration getCConf() {
    String conf = hConf.get(HCONF_ATTR_CCONF);
    Preconditions.checkArgument(conf != null, "No CConfiguration available");
    return CConfiguration.create(new ByteArrayInputStream(conf.getBytes(Charsets.UTF_8)));
  }

  private void setTx(Transaction tx) {
    hConf.set(HCONF_ATTR_NEW_TX, GSON.toJson(tx));
  }

  /**
   * Returns the {@link Transaction} information stored inside the job {@link Configuration}.
   */
  public Transaction getTx() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_NEW_TX), Transaction.class);
  }
}
