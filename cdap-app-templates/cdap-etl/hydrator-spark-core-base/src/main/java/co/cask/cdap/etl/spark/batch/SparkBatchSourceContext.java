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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.MessagePublisher;
import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.etl.api.StageSubmitterContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.batch.AbstractBatchContext;
import co.cask.cdap.etl.common.ExternalDatasets;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.spec.StageSpec;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Default implementation of {@link BatchSourceContext} for spark contexts.
 */
public class SparkBatchSourceContext extends AbstractBatchContext
  implements BatchSourceContext, StageSubmitterContext {
  private final SparkBatchSourceFactory sourceFactory;
  private final boolean isPreviewEnabled;
  private final SparkClientContext sparkContext;

  public SparkBatchSourceContext(SparkBatchSourceFactory sourceFactory, SparkClientContext sparkContext,
                                 PipelineRuntime pipelineRuntime, DatasetContext datasetContext, StageSpec stageSpec) {
    super(pipelineRuntime, stageSpec, datasetContext, sparkContext.getAdmin());
    this.sparkContext = sparkContext;
    this.sourceFactory = sourceFactory;
    this.isPreviewEnabled = sparkContext.getDataTracer(stageSpec.getName()).isEnabled();
  }

  @Override
  public void setInput(Input input) {
    Input trackableInput = ExternalDatasets.makeTrackable(admin, suffixInput(input));
    sourceFactory.addInput(getStageName(), trackableInput);
  }

  @Override
  public boolean isPreviewEnabled() {
    return isPreviewEnabled;
  }

  private Input suffixInput(Input input) {
    String suffixedAlias = String.format("%s-%s", input.getAlias(), UUID.randomUUID());
    return input.alias(suffixedAlias);
  }

  @Override
  public MessagePublisher getMessagePublisher() {
    return sparkContext.getMessagePublisher();
  }

  @Override
  public MessagePublisher getDirectMessagePublisher() {
    return sparkContext.getDirectMessagePublisher();
  }

  @Override
  public MessageFetcher getMessageFetcher() {
    return sparkContext.getMessageFetcher();
  }

  @Override
  public void createTopic(String topic) throws TopicAlreadyExistsException, IOException {
    sparkContext.getAdmin().createTopic(topic);
  }

  @Override
  public void createTopic(String topic,
                          Map<String, String> properties) throws TopicAlreadyExistsException, IOException {
    sparkContext.getAdmin().createTopic(topic, properties);
  }

  @Override
  public Map<String, String> getTopicProperties(String topic) throws TopicNotFoundException, IOException {
    return sparkContext.getAdmin().getTopicProperties(topic);
  }

  @Override
  public void updateTopic(String topic, Map<String, String> properties) throws TopicNotFoundException, IOException {
    sparkContext.getAdmin().updateTopic(topic, properties);
  }

  @Override
  public void deleteTopic(String topic) throws TopicNotFoundException, IOException {
    sparkContext.getAdmin().deleteTopic(topic);
  }
}
