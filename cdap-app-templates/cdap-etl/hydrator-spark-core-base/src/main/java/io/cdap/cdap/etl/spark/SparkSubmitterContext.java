/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.SparkClientContext;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.batch.AbstractBatchContext;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;

import java.io.IOException;
import java.util.Map;

/**
 * The submitter context can be used for both batch and realtime spark pipelines
 */
public class SparkSubmitterContext extends AbstractBatchContext implements StageSubmitterContext {
  private final MessagingContext messagingContext;

  public SparkSubmitterContext(SparkClientContext sparkContext,
                               PipelineRuntime pipelineRuntime,
                               DatasetContext datasetContext, StageSpec stageSpec) {
    super(pipelineRuntime, stageSpec, datasetContext, sparkContext.getAdmin());
    this.messagingContext = sparkContext;
  }

  public SparkSubmitterContext(JavaSparkExecutionContext sparkContext,
                               PipelineRuntime pipelineRuntime,
                               DatasetContext datasetContext, StageSpec stageSpec) {
    super(pipelineRuntime, stageSpec, datasetContext, sparkContext.getAdmin());
    this.messagingContext = sparkContext.getMessagingContext();
  }

  @Override
  public void createTopic(String topic) throws TopicAlreadyExistsException, IOException {
    admin.createTopic(topic);
  }

  @Override
  public void createTopic(String topic,
                          Map<String, String> properties) throws TopicAlreadyExistsException, IOException {
    admin.createTopic(topic, properties);
  }

  @Override
  public Map<String, String> getTopicProperties(String topic) throws TopicNotFoundException, IOException {
    return admin.getTopicProperties(topic);
  }

  @Override
  public void updateTopic(String topic, Map<String, String> properties) throws TopicNotFoundException, IOException {
    admin.updateTopic(topic, properties);
  }

  @Override
  public void deleteTopic(String topic) throws TopicNotFoundException, IOException {
    admin.deleteTopic(topic);
  }

  @Override
  public MessagePublisher getMessagePublisher() {
    return messagingContext.getMessagePublisher();
  }

  @Override
  public MessagePublisher getDirectMessagePublisher() {
    return messagingContext.getDirectMessagePublisher();
  }

  @Override
  public MessageFetcher getMessageFetcher() {
    return messagingContext.getMessageFetcher();
  }
}
