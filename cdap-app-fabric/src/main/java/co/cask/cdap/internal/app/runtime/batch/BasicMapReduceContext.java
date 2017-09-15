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

import co.cask.cdap.api.ProgramState;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.data.batch.DatasetOutputCommitter;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.WeakReferenceDelegatorClassLoader;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.batch.dataset.DatasetInputFormatProvider;
import co.cask.cdap.internal.app.runtime.batch.dataset.input.MapperInput;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.Outputs;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.ProvidedOutput;
import co.cask.cdap.internal.app.runtime.batch.stream.StreamInputFormatProvider;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.logging.context.MapReduceLoggingContext;
import co.cask.cdap.logging.context.WorkflowProgramLoggingContext;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Mapreduce job runtime context
 */
final class BasicMapReduceContext extends AbstractContext implements MapReduceContext {

  private final MapReduceSpecification spec;
  private final LoggingContext loggingContext;
  private final WorkflowProgramInfo workflowProgramInfo;
  private final StreamAdmin streamAdmin;
  private final File pluginArchive;
  private final Map<String, LocalizeResource> resourcesToLocalize;

  // key is input name, value is the MapperInput (configuration info) for that input
  private final Map<String, MapperInput> inputs;
  private final Map<String, ProvidedOutput> outputs;

  private Job job;
  private Resources mapperResources;
  private Resources reducerResources;
  private ProgramState state;
  private MapReduceClassLoader mapReduceClassLoader;

  BasicMapReduceContext(Program program, ProgramOptions programOptions,
                        CConfiguration cConf,
                        MapReduceSpecification spec,
                        @Nullable WorkflowProgramInfo workflowProgramInfo,
                        DiscoveryServiceClient discoveryServiceClient,
                        MetricsCollectionService metricsCollectionService,
                        TransactionSystemClient txClient,
                        DatasetFramework dsFramework,
                        StreamAdmin streamAdmin,
                        @Nullable File pluginArchive,
                        @Nullable PluginInstantiator pluginInstantiator,
                        SecureStore secureStore,
                        SecureStoreManager secureStoreManager,
                        MessagingService messagingService) {
    super(program, programOptions, cConf, spec.getDataSets(), dsFramework, txClient, discoveryServiceClient, false,
          metricsCollectionService, createMetricsTags(workflowProgramInfo), secureStore, secureStoreManager,
          messagingService, pluginInstantiator);

    this.workflowProgramInfo = workflowProgramInfo;
    this.loggingContext = createLoggingContext(program.getId(), getRunId(), workflowProgramInfo);
    this.spec = spec;
    this.mapperResources = SystemArguments.getResources(
      RuntimeArguments.extractScope("task", "mapper", getRuntimeArguments()), spec.getMapperResources());
    this.reducerResources = SystemArguments.getResources(
      RuntimeArguments.extractScope("task", "reducer", getRuntimeArguments()), spec.getReducerResources());
    this.streamAdmin = streamAdmin;
    this.pluginArchive = pluginArchive;
    this.resourcesToLocalize = new HashMap<>();

    this.inputs = new HashMap<>();
    this.outputs = new LinkedHashMap<>();

    if (spec.getInputDataSet() != null) {
      addInput(Input.ofDataset(spec.getInputDataSet()));
    }
    if (spec.getOutputDataSet() != null) {
      addOutput(Output.ofDataset(spec.getOutputDataSet()));
    }
  }

  private LoggingContext createLoggingContext(ProgramId programId, RunId runId,
                                              @Nullable WorkflowProgramInfo workflowProgramInfo) {
    if (workflowProgramInfo == null) {
      return new MapReduceLoggingContext(programId.getNamespace(), programId.getApplication(),
                                         programId.getProgram(), runId.getId());
    }

    ProgramId workflowProramId = programId.getParent().workflow(workflowProgramInfo.getName());

    return new WorkflowProgramLoggingContext(workflowProramId.getNamespace(), workflowProramId.getApplication(),
                                             workflowProramId.getProgram(), workflowProgramInfo.getRunId().getId(),
                                             ProgramType.MAPREDUCE, programId.getProgram(), runId.getId());
  }

  @Override
  public String toString() {
    return String.format("name=%s, jobId=%s, %s", spec.getName(),
                         job == null ? null : job.getJobID(), super.toString());
  }

  @Override
  public MapReduceSpecification getSpecification() {
    return spec;
  }

  /**
   * Returns the WorkflowToken if the MapReduce program is executed as a part of the Workflow.
   */
  @Override
  @Nullable
  public BasicWorkflowToken getWorkflowToken() {
    return workflowProgramInfo == null ? null : workflowProgramInfo.getWorkflowToken();
  }

  public void setJob(Job job) {
    this.job = job;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getHadoopJob() {
    return (T) job;
  }

  @Override
  public void addInput(Input input) {
    addInput(input, null);
  }

  @SuppressWarnings("unchecked")
  private void addInput(String alias, InputFormatProvider inputFormatProvider, @Nullable Class<?> mapperClass) {
    if (mapperClass != null && !Mapper.class.isAssignableFrom(mapperClass)) {
      throw new IllegalArgumentException("Specified mapper class must extend Mapper.");
    }
    if (inputs.containsKey(alias)) {
      throw new IllegalArgumentException("Input already configured: " + alias);
    }
    inputs.put(alias, new MapperInput(alias, inputFormatProvider, (Class<? extends Mapper>) mapperClass));
  }

  @Override
  public void addInput(Input input, @Nullable Class<?> mapperCls) {
    if (input instanceof Input.DatasetInput) {
      Input.DatasetInput datasetInput = (Input.DatasetInput) input;
      Input.InputFormatProviderInput createdInput = createInput(datasetInput);
      addInput(createdInput.getAlias(), createdInput.getInputFormatProvider(), mapperCls);
    } else if (input instanceof Input.StreamInput) {
      Input.StreamInput streamInput = (Input.StreamInput) input;
      String namespace = streamInput.getNamespace();
      if (namespace == null) {
        namespace = getProgram().getNamespaceId();
      }
      addInput(input.getAlias(),
               new StreamInputFormatProvider(new NamespaceId(namespace), streamInput, streamAdmin),
               mapperCls);
    } else if (input instanceof Input.InputFormatProviderInput) {
      addInput(input.getAlias(), ((Input.InputFormatProviderInput) input).getInputFormatProvider(), mapperCls);
    } else {
      // shouldn't happen unless user defines their own Input class
      throw new IllegalArgumentException(String.format("Input %s has unknown input class %s",
                                                       input.getName(), input.getClass().getCanonicalName()));
    }
  }

  @Override
  public void addOutput(Output output) {
    String alias = output.getAlias();
    if (this.outputs.containsKey(alias)) {
      throw new IllegalArgumentException("Output already configured: " + alias);
    }

    ProvidedOutput providedOutput;
    if (output instanceof Output.DatasetOutput) {
      providedOutput = Outputs.transform((Output.DatasetOutput) output, this);
    } else if (output instanceof Output.OutputFormatProviderOutput) {
      OutputFormatProvider outputFormatProvider =
        ((Output.OutputFormatProviderOutput) output).getOutputFormatProvider();
      if (outputFormatProvider instanceof DatasetOutputCommitter) {
        // disallow user from adding a DatasetOutputCommitter as an OutputFormatProviderOutput because we would not
        // be able to call its methods in MainOutputCommitter. It needs to be a DatasetOutput.
        throw new IllegalArgumentException("Cannot add a DatasetOutputCommitter as an OutputFormatProviderOutput. " +
                                             "Add the output as a DatasetOutput.");
      }
      providedOutput = new ProvidedOutput(output, outputFormatProvider);
    } else {
      // shouldn't happen unless user defines their own Output class
      throw new IllegalArgumentException(String.format("Output %s has unknown output class %s",
                                                       output.getName(), output.getClass().getCanonicalName()));
    }

    this.outputs.put(alias, providedOutput);
  }

  /**
   * @return a mapping from input name to the MapperInputs for the MapReduce job
   */
  Map<String, MapperInput> getMapperInputs() {
    return ImmutableMap.copyOf(inputs);
  }

  /**
   * @return a map from output name to provided output for the MapReduce job
   */
  List<ProvidedOutput> getOutputs() {
    return new ArrayList<>(outputs.values());
  }

  @Override
  public void setMapperResources(Resources resources) {
    this.mapperResources = resources;
  }

  @Override
  public void setReducerResources(Resources resources) {
    this.reducerResources = resources;
  }

  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  public Resources getMapperResources() {
    return mapperResources;
  }

  public Resources getReducerResources() {
    return reducerResources;
  }

  public File getPluginArchive() {
    return pluginArchive;
  }

  /**
   * Returns the information about Workflow if the MapReduce program is executed
   * as a part of it, otherwise {@code null} is returned.
   */
  @Override
  @Nullable
  public WorkflowProgramInfo getWorkflowInfo() {
    return workflowProgramInfo;
  }

  @Override
  public void localize(String name, URI uri) {
    localize(name, uri, false);
  }

  @Override
  public void localize(String name, URI uri, boolean archive) {
    resourcesToLocalize.put(name, new LocalizeResource(uri, archive));
  }

  @Override
  protected ClassLoader createProgramInvocationClassLoader() {
    if (mapReduceClassLoader == null) {
      // This shouldn't happen. Just to prevent bug and be able to catch it in unit-test.
      throw new IllegalStateException("The MapReduceClassLoader is not yet set");
    }
    return new WeakReferenceDelegatorClassLoader(mapReduceClassLoader);
  }

  void setMapReduceClassLoader(MapReduceClassLoader classLoader) {
    this.mapReduceClassLoader = classLoader;
  }

  Map<String, LocalizeResource> getResourcesToLocalize() {
    return resourcesToLocalize;
  }


  private Input.InputFormatProviderInput createInput(Input.DatasetInput datasetInput) {
    String datasetName = datasetInput.getName();
    Map<String, String> datasetArgs = datasetInput.getArguments();
    // keep track of the original alias to set it on the created Input before returning it
    String originalAlias = datasetInput.getAlias();

    Dataset dataset;
    if (datasetInput.getNamespace() == null) {
      dataset = getDataset(datasetName, datasetArgs, AccessType.READ);
    } else {
      dataset = getDataset(datasetInput.getNamespace(), datasetName, datasetArgs, AccessType.READ);
    }
    DatasetInputFormatProvider datasetInputFormatProvider =
      new DatasetInputFormatProvider(datasetInput.getNamespace(), datasetName, datasetArgs, dataset,
                                     datasetInput.getSplits(), MapReduceBatchReadableInputFormat.class);
    return (Input.InputFormatProviderInput) Input.of(datasetName, datasetInputFormatProvider).alias(originalAlias);
  }

  /**
   * Sets the current state of the program.
   */
  void setState(ProgramState state) {
    this.state = state;
  }

  @Override
  public ProgramState getState() {
    return state;
  }

  private static Map<String, String> createMetricsTags(@Nullable WorkflowProgramInfo workflowProgramInfo) {
    if (workflowProgramInfo != null) {
      return workflowProgramInfo.updateMetricsTags(new HashMap<String, String>());
    }
    return Collections.emptyMap();
  }
}
