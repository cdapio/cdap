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

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.auditlogging.AuditLogPublisherService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The default implementation of {@link AuditLogPublisherService} , which runs in the app-fabric and receives
 * a collection of {@link AuditLogContext}s . This class is responsible to store them in a queue and timely publish
 * them to an SPI.
 */
@Singleton
public class DefaultAuditLogPublisherService extends AbstractScheduledService
  implements AuditLogPublisherService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuditLogPublisherService.class);
  private static final int MAX_QUEUE_STORAGE_COUNT = 10;
  private final int publishIntervalSeconds;
  private static AtomicBoolean publishing = new AtomicBoolean(false);
  private ScheduledExecutorService executor;

  Queue<AuditLogContext> auditLogContextQueue = new LinkedBlockingDeque<>();

  @Inject
  public DefaultAuditLogPublisherService(CConfiguration conf) {
    this.publishIntervalSeconds = conf.getInt(Constants.AuditLogging.AUDIT_LOG_PUBLISH_INTERVAL_SECONDS);
    LOG.warn("SANKET_LOG_1 : in constructor : publishIntervalSeconds : {}", this.publishIntervalSeconds);
  }

  @Override
  protected void runOneIteration() throws Exception {
    publish();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, publishIntervalSeconds, TimeUnit.SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("audit-log-publisher"));
    return executor;
  }

  @Override
  public synchronized void publish() {
    publishing.set(true);
    LOG.warn("SANKET_LOG_2 : publish");
    publishing.set(false);
  }

  @Override
  public void addAuditContexts(Queue<AuditLogContext> q) {
    LOG.warn("SANKET_LOG_3 : adding : " + q.size());
    auditLogContextQueue.addAll(q);

    //Trigger a publish call if there is an outburst of events, and it gets accumulated within `publishIntervalSeconds`
    if (auditLogContextQueue.size() > MAX_QUEUE_STORAGE_COUNT && !publishing.get()) {
      publish();
    }
  }

  @Override
  protected void shutDown() throws Exception {
    if (!auditLogContextQueue.isEmpty()) {
      publish();
    }

    if (executor != null) {
      executor.shutdownNow();
    }
  }

}