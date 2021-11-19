/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.runtimejob;

import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.provisioner.tether.TetherConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tether runtime job manager. This class is sending runtime job details to the tethered CDAP instance through TMS.
 * An instance of this class is created by {@code TetherProvisioner}.
 */
public class TetherRuntimeJobManager implements RuntimeJobManager {

  private static final Logger LOG = LoggerFactory.getLogger(TetherRuntimeJobManager.class);

  private final MessagingService messagingService;
  private final MultiThreadMessagingContext messagingContext;
  private final Map<String, String> properties;
  private final String tetheredInstanceName;
  private final String tetheredNamespace;

  public TetherRuntimeJobManager(MessagingService messagingService, Map<String, String> properties) {
    this.messagingService = messagingService;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.properties = properties;
    TetherConf conf = TetherConf.fromProperties(properties);
    this.tetheredInstanceName = conf.getTetheredInstanceName();
    this.tetheredNamespace = conf.getTetheredNamespace();
  }

  @Override
  public void launch(RuntimeJobInfo runtimeJobInfo) throws IOException {
    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();
    LOG.debug("Launching run {} with following configurations: tethered instance name {}, tethered namespace {}.",
              runInfo.getRun(), tetheredInstanceName, tetheredNamespace);
    // create RUN_PIPELINE topic for the namespace if it does not exist
    TopicId topicId = new TopicId(runInfo.getNamespace(), "run_pipeline");
    TopicMetadata topicMetadata = new TopicMetadata(topicId, properties);
    try {
      messagingService.createTopic(topicMetadata);
    } catch (TopicAlreadyExistsException e) {
      LOG.warn("Topic {} already exists", topicId);
    } catch (IOException e) {
      LOG.error("Failed to create topic {}", topicId, e);
      throw e;
    }

    MessagePublisher publisher = messagingContext.getMessagePublisher();
    try {
      // TODO: determine contents of run_pipeline message
      publisher.publish(runInfo.getNamespace(), topicId.getTopic(), "");
      LOG.debug("Successfully published message to {}", topicId);
    } catch (Exception e) {
      LOG.error("Failed to publish to topic {}", topicId, e);
      throw new IOException(e);
    }
  }

  @Override
  public Optional<RuntimeJobDetail> getDetail(ProgramRunInfo programRunInfo) throws Exception {
    // TODO: get job details from tethered instance
    return Optional.empty();
  }

  @Override
  public List<RuntimeJobDetail> list() throws Exception {
    // TODO: get job details for all running jobs in tethered instance
    return null;
  }

  @Override
  public void stop(ProgramRunInfo programRunInfo) throws Exception {
    RuntimeJobDetail jobDetail = getDetail(programRunInfo).orElse(null);
    if (jobDetail == null) {
      return;
    }
    RuntimeJobStatus status = jobDetail.getStatus();
    if (status.isTerminated() || status == RuntimeJobStatus.STOPPING) {
      return;
    }
    // TODO: stop running job in tethered instance
  }

  @Override
  public void kill(ProgramRunInfo programRunInfo) throws Exception {
    stop(programRunInfo);
  }

  @Override
  public void close() {
    // no-op
  }
}
