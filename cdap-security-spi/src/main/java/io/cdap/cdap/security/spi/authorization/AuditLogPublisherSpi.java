package io.cdap.cdap.security.spi.authorization;

import java.util.List;

/**
 * Delegates / Publishes a list of String of {@link AuditLogContext} obtained from prior operation
 */
public interface AuditLogPublisherSpi {

  public enum PublishStatus {
    PUBLISHED,
    FAILED,
    CAN_RETRY
  }

  /**
   * Publishes a list of String of {@link AuditLogContext} obtained from prior operation
   *
   * @return PublishStatus
   */
  PublishStatus publish(List<String> auditLogList);

}
