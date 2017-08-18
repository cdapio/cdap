/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.MessagePublisher;
import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.etl.api.StageSubmitterContext;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.batch.AbstractBatchContext;
import co.cask.cdap.etl.batch.preview.NullOutputFormatProvider;
import co.cask.cdap.etl.common.ExternalDatasets;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.common.plugin.Caller;
import co.cask.cdap.etl.common.plugin.NoStageLoggingCaller;
import co.cask.cdap.etl.spec.StageSpec;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Abstract implementation of {@link BatchContext} using {@link MapReduceContext}.
 */
public class MapReduceBatchContext extends AbstractBatchContext
  implements BatchSinkContext, BatchSourceContext, StageSubmitterContext {
  private static final Caller CALLER = NoStageLoggingCaller.wrap(Caller.DEFAULT);
  private final MapReduceContext mrContext;
  private final boolean isPreviewEnabled;
  private final Set<String> outputNames;
  private final Set<String> inputNames;
  private final Set<String> connectorDatasets;

  public MapReduceBatchContext(MapReduceContext context, PipelineRuntime pipelineRuntime, StageSpec stageSpec,
                               Set<String> connectorDatasets) {
    super(pipelineRuntime, stageSpec, context, context.getAdmin());
    this.mrContext = context;
    this.outputNames = new HashSet<>();
    this.inputNames = new HashSet<>();
    this.isPreviewEnabled = context.getDataTracer(stageSpec.getName()).isEnabled();
    this.connectorDatasets = Collections.unmodifiableSet(connectorDatasets);
  }

  @Override
  public void setInput(final Input input) {
    Input trackableInput = CALLER.callUnchecked(new Callable<Input>() {
      @Override
      public Input call() throws Exception {
        Input trackableInput = ExternalDatasets.makeTrackable(mrContext.getAdmin(), suffixInput(input));
        mrContext.addInput(trackableInput);
        return trackableInput;
      }
    });
    inputNames.add(trackableInput.getAlias());
  }

  @Override
  public void addOutput(final String datasetName) {
    addOutput(datasetName, Collections.<String, String>emptyMap());
  }

  @Override
  public void addOutput(final String datasetName, final Map<String, String> arguments) {
    String alias = CALLER.callUnchecked(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Output output = suffixOutput(getOutput(Output.ofDataset(datasetName, arguments)));
        mrContext.addOutput(output);
        return output.getAlias();
      }
    });
    outputNames.add(alias);
  }

  @Override
  public void addOutput(final String outputName, final OutputFormatProvider outputFormatProvider) {
    String alias = CALLER.callUnchecked(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Output output = suffixOutput(getOutput(Output.of(outputName, outputFormatProvider)));
        mrContext.addOutput(output);
        return output.getAlias();
      }
    });
    outputNames.add(alias);
  }

  @Override
  public void addOutput(final Output output) {
    final Output actualOutput = suffixOutput(getOutput(output));
    Output trackableOutput = CALLER.callUnchecked(new Callable<Output>() {
      @Override
      public Output call() throws Exception {
        Output trackableOutput = isPreviewEnabled ? actualOutput : ExternalDatasets.makeTrackable(mrContext.getAdmin(),
                                                                                                  actualOutput);
        mrContext.addOutput(trackableOutput);
        return trackableOutput;
      }
    });
    outputNames.add(trackableOutput.getAlias());
  }

  @Override
  public boolean isPreviewEnabled() {
    return isPreviewEnabled;
  }

  /**
   * @return set of inputs that were added
   */
  public Set<String> getInputNames() {
    return inputNames;
  }

  /**
   * @return set of outputs that were added
   */
  public Set<String> getOutputNames() {
    return outputNames;
  }

  /**
   * Suffix the alias of {@link Output} so that aliases of outputs are unique.
   */
  private Output suffixOutput(Output output) {
    String suffixedAlias = String.format("%s-%s", output.getAlias(), UUID.randomUUID());
    return output.alias(suffixedAlias);
  }

  /**
   * Suffix the alias of {@link Input} so that aliases of inputs are unique.
   */
  private Input suffixInput(Input input) {
    String suffixedAlias = String.format("%s-%s", input.getAlias(), UUID.randomUUID());
    return input.alias(suffixedAlias);
  }

  /**
   * Get the output, if preview is enabled, return the output with a {@link NullOutputFormatProvider}.
   */
  private Output getOutput(Output output) {
    // Do no return NullOutputFormat for connector datasets
    if (isPreviewEnabled && !connectorDatasets.contains(output.getName())) {
      return Output.of(output.getName(), new NullOutputFormatProvider());
    }
    return output;
  }

  @Override
  public MessagePublisher getMessagePublisher() {
    return mrContext.getMessagePublisher();
  }

  @Override
  public MessagePublisher getDirectMessagePublisher() {
    return mrContext.getDirectMessagePublisher();
  }

  @Override
  public MessageFetcher getMessageFetcher() {
    return mrContext.getMessageFetcher();
  }

  @Override
  public void createTopic(String topic) throws TopicAlreadyExistsException, IOException {
    mrContext.getAdmin().createTopic(topic);
  }

  @Override
  public void createTopic(String topic,
                          Map<String, String> properties) throws TopicAlreadyExistsException, IOException {
    mrContext.getAdmin().createTopic(topic, properties);
  }

  @Override
  public Map<String, String> getTopicProperties(String topic) throws TopicNotFoundException, IOException {
    return mrContext.getAdmin().getTopicProperties(topic);
  }

  @Override
  public void updateTopic(String topic, Map<String, String> properties) throws TopicNotFoundException, IOException {
    mrContext.getAdmin().updateTopic(topic, properties);
  }

  @Override
  public void deleteTopic(String topic) throws TopicNotFoundException, IOException {
    mrContext.getAdmin().deleteTopic(topic);
  }
}
