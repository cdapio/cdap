package com.continuuity.logging.context;

import com.continuuity.common.logging.ApplicationLoggingContext;

/**
 * A logging context when the type of entity is not known. This logging context has limited functionality.
 */
public class GenericLoggingContext extends ApplicationLoggingContext {
  public static final String TAG_ENTITY_ID = ".entityId";

  /**
   * Constructs the GenericLoggingContext.
   * @param accountId account id
   * @param applicationId application id
   * @param entityId flow entity id
   */
  public GenericLoggingContext(final String accountId,
                                 final String applicationId,
                                 final String entityId) {
    super(accountId, applicationId);
    setSystemTag(TAG_ENTITY_ID, entityId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_ENTITY_ID));
  }

  @Override
  public String getLogPathFragment() {
    throw new UnsupportedOperationException("GenericLoggingContext does not support this");
  }
}
