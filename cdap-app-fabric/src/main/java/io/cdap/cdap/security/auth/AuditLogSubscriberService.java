/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;


import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.twill.internal.CompositeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Audit Log Service that creates children services, each to handle a single partition of audit log events topic.
 */
public class AuditLogSubscriberService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AuditLogSubscriberService.class);

  private final MessagingService messagingService;
  private final CConfiguration cConf;
  private final TransactionRunner transactionRunner;
  private final AccessControllerInstantiator accessControllerInstantiator;
  private final MetricsCollectionService metricsCollectionService;
  private Service delegate;

  @Inject
  public AuditLogSubscriberService(CConfiguration cConf, MessagingService messagingService,
                                   MetricsCollectionService metricsCollectionService,
                                   TransactionRunner transactionRunner,
                                   AccessControllerInstantiator accessControllerInstantiator) {
    this.messagingService = messagingService;
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.transactionRunner = transactionRunner;
    this.accessControllerInstantiator = accessControllerInstantiator;
  }

  @Override
  protected void startUp() throws Exception {
    List<Service> children = new ArrayList<>();
    String topicPrefix = cConf.get(Constants.AuditLogging.AUDIT_LOG_EVENT_TOPIC);
    int numPartitions = cConf.getInt(Constants.AuditLogging.AUDIT_LOG_EVENT_NUM_PARTITIONS);
    IntStream.range(0, numPartitions)
      .forEach(i -> children.add(createChildService(topicPrefix + i)));
    delegate = new CompositeService(children);

    delegate.startAndWait();
    LOG.debug("Started Audit Log subscriber service for {} partitions.", numPartitions);
  }

  @Override
  protected void shutDown() throws Exception {
    delegate.stopAndWait();
  }

  private AuditLogSingleTopicSubscriberService createChildService(String topicName) {

    return new AuditLogSingleTopicSubscriberService(
      this.cConf,
      this.messagingService,
      this.metricsCollectionService,
      this.transactionRunner,
      this.accessControllerInstantiator,
      topicName
    );
  }
}
