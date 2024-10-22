package io.cdap.cdap.security.spi.authorization;

import io.cdap.cdap.api.annotation.Beta;

import java.util.Queue;

/**
 * An SPI that delegates the collection of {@link AuditLogContext}s to an extension that will publish the log events
 * to the respective destination.
 */
@Beta
public interface AuditLoggerSpi {
  /**
   * The status of a call for authorization check.
   */
  enum PublishStatus {
    PUBLISHED,
    UNSUCCESSFUL
  }

  /**
   * TODO : THIS IS WIP : Needs to be modified based on how auth extension works.
   * @return {@link PublishStatus}
   */
  PublishStatus publish(Queue<AuditLogContext> auditLogContexts);

}