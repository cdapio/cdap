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
    LOG.warn("SANKET_LOG_1 : in constructer");
  }

  @Override
  public void publish() {
    LOG.warn("SANKET_LOG_1 : publish");
  }

  @Override
  public void addAuditContexts(Queue<AuditLogContext> q) {
    LOG.warn("SANKET_LOG_1 : adding");
    auditLogContextQueue.add(AuditLogContext.Builder.defaultNotRequired());
    auditLogContextQueue.add(AuditLogContext.Builder.defaultNotRequired());
  }

  @Override
  protected void run() throws Exception {
    LOG.warn("SANKET_LOG_1 : run");
  }
}