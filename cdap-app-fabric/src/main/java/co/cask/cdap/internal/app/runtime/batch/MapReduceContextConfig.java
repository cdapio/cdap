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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.proto.id.PreviewId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tephra.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Helper class for getting and setting specific config settings for a job context.
 */
final class MapReduceContextConfig {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceContextConfig.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .create();
  private static final Type PLUGIN_MAP_TYPE = new TypeToken<Map<String, Plugin>>() {
  }.getType();

  private static final String HCONF_ATTR_APP_SPEC = "cdap.mapreduce.app.spec";
  private static final String HCONF_ATTR_PROGRAM_ID = "cdap.mapreduce.program.id";
  private static final String HCONF_ATTR_WORKFLOW_INFO = "cdap.mapreduce.workflow.info";
  private static final String HCONF_ATTR_PLUGINS = "cdap.mapreduce.plugins";
  private static final String HCONF_ATTR_PROGRAM_JAR_URI = "cdap.mapreduce.program.jar.uri";
  private static final String HCONF_ATTR_CCONF = "cdap.mapreduce.cconf";
  private static final String HCONF_ATTR_NEW_TX = "cdap.mapreduce.newtx";
  private static final String HCONF_ATTR_LOCAL_FILES = "cdap.mapreduce.local.files";
  private static final String HCONF_ATTR_PROGRAM_OPTIONS = "cdap.mapreduce.program.options";
  private static final String HCONF_ATTR_PREVIEW_ID = "hconf.preview.id";

  private final Configuration hConf;

  MapReduceContextConfig(Configuration hConf) {
    this.hConf = hConf;
  }

  Configuration getHConf() {
    return hConf;
  }

  /**
   * Updates the {@link Configuration} of this class with the given paramters.
   *
   * @param context                the context for the MapReduce program
   * @param conf                   the CDAP configuration
   * @param tx                     the long transaction created for the MapReduce program
   * @param programJarURI          The URI of the program JAR
   * @param localizedUserResources the localized resources for the MapReduce program
   */
  public void set(BasicMapReduceContext context, CConfiguration conf, Transaction tx, URI programJarURI,
                  Map<String, String> localizedUserResources) {
    setProgramOptions(context.getProgramOptions());
    setProgramId(context.getProgram().getId().toEntityId());
    setApplicationSpecification(context.getApplicationSpecification());
    setWorkflowProgramInfo(context.getWorkflowInfo());
    setPlugins(context.getApplicationSpecification().getPlugins());
    setProgramJarURI(programJarURI);
    setConf(conf);
    setTx(tx);
    setLocalizedResources(localizedUserResources);
    setPreviewId(context.getPreviewId());
  }

  private void setProgramId(ProgramId programId) {
    hConf.set(HCONF_ATTR_PROGRAM_ID, GSON.toJson(programId));
  }

  /**
   * Serialize the {@link ApplicationSpecification} to the configuration.
   */
  private void setApplicationSpecification(ApplicationSpecification spec) {
    hConf.set(HCONF_ATTR_APP_SPEC, GSON.toJson(spec, ApplicationSpecification.class));
  }

  /**
   * Returns the {@link ProgramId} for the MapReduce program.
   */
  public ProgramId getProgramId() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_PROGRAM_ID), ProgramId.class);
  }

  /**
   * @return the {@link ApplicationSpecification} stored in the configuration.
   */
  public ApplicationSpecification getApplicationSpecification() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_APP_SPEC), ApplicationSpecification.class);
  }

  private void setWorkflowProgramInfo(@Nullable WorkflowProgramInfo info) {
    if (info != null) {
      hConf.set(HCONF_ATTR_WORKFLOW_INFO, GSON.toJson(info));
    }
  }

  /**
   * Returns the {@link WorkflowProgramInfo} if it is running inside Workflow or {@code null} if not.
   */
  @Nullable
  WorkflowProgramInfo getWorkflowProgramInfo() {
    String info = hConf.get(HCONF_ATTR_WORKFLOW_INFO);
    if (info == null) {
      return null;
    }
    WorkflowProgramInfo workflowProgramInfo = GSON.fromJson(info, WorkflowProgramInfo.class);
    workflowProgramInfo.getWorkflowToken().disablePut();
    return workflowProgramInfo;
  }

  private void setPlugins(Map<String, Plugin> plugins) {
    hConf.set(HCONF_ATTR_PLUGINS, GSON.toJson(plugins, PLUGIN_MAP_TYPE));
  }

  /**
   * Returns the plugins being used in the MapReduce program.
   */
  public Map<String, Plugin> getPlugins() {
    String spec = hConf.get(HCONF_ATTR_PLUGINS);
    if (spec == null) {
      return ImmutableMap.of();
    }
    return GSON.fromJson(spec, PLUGIN_MAP_TYPE);
  }

  private void setProgramJarURI(URI programJarURI) {
    hConf.set(HCONF_ATTR_PROGRAM_JAR_URI, programJarURI.toASCIIString());
  }

  /**
   * Returns the URI of where the program JAR is.
   */
  URI getProgramJarURI() {
    return URI.create(hConf.get(HCONF_ATTR_PROGRAM_JAR_URI));
  }

  /**
   * Returns the file name of the program JAR.
   */
  String getProgramJarName() {
    return new Path(getProgramJarURI()).getName();
  }

  private void setLocalizedResources(Map<String, String> localizedUserResources) {
    hConf.set(HCONF_ATTR_LOCAL_FILES, GSON.toJson(localizedUserResources));
  }

  Map<String, File> getLocalizedResources() {
    Map<String, String> nameToPath = GSON.fromJson(hConf.get(HCONF_ATTR_LOCAL_FILES),
                                                   new TypeToken<Map<String, String>>() {
                                                   }.getType());
    Map<String, File> nameToFile = new HashMap<>();
    for (Map.Entry<String, String> entry : nameToPath.entrySet()) {
      nameToFile.put(entry.getKey(), new File(entry.getValue()));
    }
    return nameToFile;
  }

  private void setProgramOptions(ProgramOptions programOptions) {
    hConf.set(HCONF_ATTR_PROGRAM_OPTIONS, GSON.toJson(programOptions, ProgramOptions.class));
  }

  /**
   * Returns the {@link ProgramOptions} from the configuration.
   */
  public ProgramOptions getProgramOptions() {
    return GSON.fromJson(hConf.get(HCONF_ATTR_PROGRAM_OPTIONS), ProgramOptions.class);
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
  CConfiguration getCConf() {
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

  private void setPreviewId(@Nullable PreviewId previewId) {
    if (previewId != null) {
      hConf.set(HCONF_ATTR_PREVIEW_ID, GSON.toJson(previewId));
    }
  }

  @Nullable
  public PreviewId getPreviewId() {
    String previewIdJson = hConf.get(HCONF_ATTR_PREVIEW_ID);
    if (previewIdJson == null) {
      return null;
    }
    return GSON.fromJson(previewIdJson, PreviewId.class);
  }
}
