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

package io.cdap.cdap.etl.batch.mapreduce;

import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.batch.BatchContext;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.batch.AbstractBatchContext;
import io.cdap.cdap.etl.batch.preview.LimitingInputFormatProvider;
import io.cdap.cdap.etl.batch.preview.NullOutputFormatProvider;
import io.cdap.cdap.etl.common.ExternalDatasets;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.plugin.Caller;
import io.cdap.cdap.etl.common.plugin.NoStageLoggingCaller;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
                               Set<String> connectorDatasets, DatasetContext datasetContext) {
    super(pipelineRuntime, stageSpec, datasetContext, context.getAdmin());
    this.mrContext = context;
    this.outputNames = new HashSet<>();
    this.inputNames = new HashSet<>();
    this.isPreviewEnabled = context.getDataTracer(stageSpec.getName()).isEnabled();
    this.connectorDatasets = Collections.unmodifiableSet(connectorDatasets);
  }

  @Override
  public void setInput(Input input) {
    Input wrapped = CALLER.callUnchecked(() -> {
      Input trackableInput = input;
      if (isPreviewEnabled && input instanceof Input.InputFormatProviderInput) {
        InputFormatProvider inputFormatProvider = ((Input.InputFormatProviderInput) input).getInputFormatProvider();
        LimitingInputFormatProvider wrapper =
          new LimitingInputFormatProvider(inputFormatProvider, getMaxPreviewRecords());
        trackableInput = Input.of(input.getName(), wrapper).alias(input.getAlias());
      }
      mrContext.addInput(trackableInput);
      return trackableInput;
    });
    inputNames.add(wrapped.getAlias());
  }

  @Override
  public void addOutput(Output output) {
    Output actualOutput = suffixOutput(getOutput(output));
    Output trackableOutput = CALLER.callUnchecked(() -> {
      Output trackableOutput1 = isPreviewEnabled ? actualOutput : ExternalDatasets.makeTrackable(mrContext.getAdmin(),
                                                                                                 actualOutput);
      mrContext.addOutput(trackableOutput1);
      return trackableOutput1;
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
