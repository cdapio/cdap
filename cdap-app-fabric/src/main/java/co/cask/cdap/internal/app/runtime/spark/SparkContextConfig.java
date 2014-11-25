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

import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.tephra.Transaction;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Helper class for getting and setting specific config settings for a spark job context
 */
public class SparkContextConfig {

  private static final Logger LOG = LoggerFactory.getLogger(SparkContextConfig.class);
  private static final Gson GSON = new Gson();

  private static final String HCONF_ATTR_RUN_ID = "hconf.program.run.id";
  private static final String HCONF_ATTR_LOGICAL_START_TIME = "hconf.program.logical.start.time";
  private static final String HCONF_ATTR_WORKFLOW_BATCH = "hconf.program.workflow.batch";
  private static final String HCONF_ATTR_ARGS = "hconf.program.args";
  private static final String HCONF_ATTR_PROGRAM_JAR_NAME = "hconf.program.jar.name";
  private static final String HCONF_ATTR_CCONF = "hconf.cconf";
  public static final String HCONF_ATTR_INPUT_SPLIT_CLASS = "hconf.program.input.split.class";
  public static final String HCONF_ATTR_INPUT_SPLITS = "hconf.program.input.splits";
  private static final String HCONF_ATTR_NEW_TX = "hconf.program.newtx.tx";
  private static final String HCONF_ATTR_PROGRAM_JAR_LOCATION = "hconf.program.jar.location";

  private static Configuration hConf;

  public static Configuration getHConf() {
    return hConf;
  }

  public SparkContextConfig(Configuration hConf) {
    SparkContextConfig.hConf = hConf;
  }

  public static void set(Configuration hadoopConf, BasicSparkContext context, CConfiguration conf, Transaction tx,
                         Location programJarCopy) {
    hConf = hadoopConf;
    setRunId(context.getRunId().getId());
    setLogicalStartTime(context.getLogicalStartTime());
    //TODO: Change this once we start supporting Spark in Workflow
    setWorkflowBatch("Not Supported");
    setArguments(context.getRuntimeArguments());
    setProgramJarName(programJarCopy.getName());
    setProgramLocation(programJarCopy.toURI());
    setConf(conf);
    setTx(tx);
  }

  private static void setArguments(Map<String, String> runtimeArgs) {
    hConf.set(HCONF_ATTR_ARGS, new Gson().toJson(runtimeArgs));
  }

  public Arguments getArguments() {
    Map<String, String> arguments = new Gson().fromJson(hConf.get(HCONF_ATTR_ARGS),
                                                        new TypeToken<Map<String, String>>() { }.getType());
    return new BasicArguments(arguments);
  }

  public URI getProgramLocation() {
    URI uri;
    try {
      uri = (new URI(hConf.get(HCONF_ATTR_PROGRAM_JAR_LOCATION)));
    } catch (URISyntaxException use) {
      LOG.error("Failed to create an URI from program location. The string violates RFC 2396", use);
      throw Throwables.propagate(use);
    }
    return uri;
  }

  private static void setRunId(String runId) {
    hConf.set(HCONF_ATTR_RUN_ID, runId);
  }

  public String getRunId() {
    return hConf.get(HCONF_ATTR_RUN_ID);
  }

  private static void setLogicalStartTime(long startTime) {
    hConf.setLong(HCONF_ATTR_LOGICAL_START_TIME, startTime);
  }

  public long getLogicalStartTime() {
    return hConf.getLong(HCONF_ATTR_LOGICAL_START_TIME, System.currentTimeMillis());
  }

  private static void setWorkflowBatch(String workflowBatch) {
    if (workflowBatch != null) {
      hConf.set(HCONF_ATTR_WORKFLOW_BATCH, workflowBatch);
    }
  }

  public String getWorkflowBatch() {
    return hConf.get(HCONF_ATTR_WORKFLOW_BATCH);
  }

  public List<Split> getInputSelection() {
    String splitClassName = hConf.get(HCONF_ATTR_INPUT_SPLIT_CLASS);
    String splitsJson = hConf.get(HCONF_ATTR_INPUT_SPLITS);
    if (splitClassName == null || splitsJson == null) {
      return Collections.emptyList();
    }

    try {
      @SuppressWarnings("unchecked")
      Class<? extends Split> splitClass =
        (Class<? extends Split>) hConf.getClassLoader().loadClass(splitClassName);
      return new Gson().fromJson(splitsJson, new ListSplitType(splitClass));
    } catch (ClassNotFoundException e) {
      LOG.warn("Class not found {}", splitClassName, e);
      throw Throwables.propagate(e);
    }
  }

  private static void setProgramLocation(URI programJarLocation) {
    hConf.set(HCONF_ATTR_PROGRAM_JAR_LOCATION, programJarLocation.toString());
  }

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
      return null;
    }
  }

  private static void setConf(CConfiguration conf) {
    StringWriter stringWriter = new StringWriter();
    try {
      conf.writeXml(stringWriter);
    } catch (IOException e) {
      LOG.error("Unable to serialize CConfiguration into xml");
      throw Throwables.propagate(e);
    }
    hConf.set(HCONF_ATTR_CCONF, stringWriter.toString());
  }

  private static void setProgramJarName(String programJarName) {
    hConf.set(HCONF_ATTR_PROGRAM_JAR_NAME, programJarName);
  }

  public CConfiguration getConf() {
    CConfiguration conf = CConfiguration.create();
    conf.addResource(new ByteArrayInputStream(hConf.get(HCONF_ATTR_CCONF).getBytes()));
    return conf;
  }

  private static void setTx(Transaction tx) {
    hConf.set(HCONF_ATTR_NEW_TX, GSON.toJson(tx));
  }

  public Transaction getTx() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_NEW_TX), Transaction.class);
  }
}
