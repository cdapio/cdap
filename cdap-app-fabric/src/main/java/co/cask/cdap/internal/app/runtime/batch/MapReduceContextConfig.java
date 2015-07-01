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
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetInputFormat;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetOutputFormat;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.templates.AdapterDefinition;
import co.cask.tephra.Transaction;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
  private static final String HCONF_ATTR_ADAPTER_SPEC = "hconf.program.adapter.spec";
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

  public Configuration getConfiguration() {
    return hConf;
  }

  public void set(BasicMapReduceContext context, CConfiguration conf, Transaction tx, URI programJarURI) {
    setRunId(context.getRunId().getId());
    setLogicalStartTime(context.getLogicalStartTime());
    setProgramNameInWorkflow(context.getProgramNameInWorkflow());
    setWorkflowToken(context.getWorkflowToken());
    setAdapterSpec(context.getAdapterSpecification());
    setArguments(context.getRuntimeArguments());
    setProgramJarURI(programJarURI);
    setConf(conf);
    setTx(tx);
    setInputSelection(context.getInputDataSelection());
  }

  private void setArguments(Map<String, String> arguments) {
    hConf.set(HCONF_ATTR_ARGS, GSON.toJson(arguments));
  }

  public Arguments getArguments() {
    Map<String, String> arguments = GSON.fromJson(hConf.get(HCONF_ATTR_ARGS),
                                                  new TypeToken<Map<String, String>>() {
                                                  }.getType());
    return new BasicArguments(arguments);
  }

  private void setRunId(String runId) {
    hConf.set(HCONF_ATTR_RUN_ID, runId);
  }

  public String getRunId() {
    return hConf.get(HCONF_ATTR_RUN_ID);
  }

  private void setLogicalStartTime(long startTime) {
    hConf.setLong(HCONF_ATTR_LOGICAL_START_TIME, startTime);
  }

  public long getLogicalStartTime() {
    return hConf.getLong(HCONF_ATTR_LOGICAL_START_TIME, System.currentTimeMillis());
  }

  private void setProgramNameInWorkflow(@Nullable String programNameInWorkflow) {
    if (programNameInWorkflow != null) {
      hConf.set(HCONF_ATTR_PROGRAM_NAME_IN_WORKFLOW, programNameInWorkflow);
    }
  }

  public String getProgramNameInWorkflow() {
    return hConf.get(HCONF_ATTR_PROGRAM_NAME_IN_WORKFLOW);
  }

  private void setWorkflowToken(@Nullable WorkflowToken workflowToken) {
    if (workflowToken != null) {
      hConf.set(HCONF_ATTR_WORKFLOW_TOKEN, GSON.toJson(workflowToken));
    }
  }

  @Nullable
  public WorkflowToken getWorkflowToken() {
    String tokenJson = hConf.get(HCONF_ATTR_WORKFLOW_TOKEN);
    if (tokenJson == null) {
      return null;
    }
    return GSON.fromJson(tokenJson, BasicWorkflowToken.class);
  }

  private void setAdapterSpec(@Nullable AdapterDefinition adapterSpec) {
    if (adapterSpec != null) {
      hConf.set(HCONF_ATTR_ADAPTER_SPEC, GSON.toJson(adapterSpec));
    }
  }

  @Nullable
  public AdapterDefinition getAdapterSpec() {
    String spec = hConf.get(HCONF_ATTR_ADAPTER_SPEC);
    if (spec == null) {
      return null;
    }
    return GSON.fromJson(spec, AdapterDefinition.class);
  }

  private void setProgramJarURI(URI programJarURI) {
    hConf.set(HCONF_ATTR_PROGRAM_JAR_URI, programJarURI.toASCIIString());
  }

  public URI getProgramJarURI() {
    return URI.create(hConf.get(HCONF_ATTR_PROGRAM_JAR_URI));
  }

  public String getProgramJarName() {
    return new Path(getProgramJarURI()).getName();
  }

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

  public CConfiguration getConf() {
    CConfiguration conf = CConfiguration.create();
    conf.addResource(new ByteArrayInputStream(hConf.get(HCONF_ATTR_CCONF).getBytes()));
    return conf;
  }

  private void setTx(Transaction tx) {
    hConf.set(HCONF_ATTR_NEW_TX, GSON.toJson(tx));
  }

  public Transaction getTx() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_NEW_TX), Transaction.class);
  }
}
