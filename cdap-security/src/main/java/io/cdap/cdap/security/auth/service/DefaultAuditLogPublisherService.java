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

package io.cdap.cdap.security.auth.service;


import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.auditlogging.AuditLogPublisherService;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

//Example: AggregatedMetricsCollectionService extends AbstractExecutionThreadService implements MetricsCollectionService

@Singleton
public class DefaultAuditLogPublisherService extends AbstractExecutionThreadService
  implements AuditLogPublisherService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuditLogPublisherService.class);

  Queue<AuditLogContext> auditLogContextQueue = new LinkedBlockingDeque<>();

  @Inject
  public DefaultAuditLogPublisherService() {
    LOG.warn("SANKET_LOG_1 : in constructor");
  }

  @Override
  public void publish() {
    LOG.warn("SANKET_LOG_2 : publish");
  }

  @Override
  public void addAuditContexts(Queue<AuditLogContext> q) {
    LOG.warn("SANKET_LOG_3 : adding : " + q.size());
    auditLogContextQueue.addAll(q);
  }

  @Override
  protected void run() throws Exception {
    LOG.warn("SANKET_LOG_1 : run");
  }
}