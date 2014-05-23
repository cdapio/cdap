package com.continuuity.common.logging;

/**
 * System Logging Context.
 */
public abstract class SystemLoggingContext extends AbstractLoggingContext {
  public static final String TAG_SYSTEM_ID = ".systemid";

  /**
   * Construct a SystemLoggingContext.
   * @param systemId system id
   */
  public SystemLoggingContext(final String systemId) {
    setSystemTag(TAG_SYSTEM_ID, systemId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s", getSystemTag(TAG_SYSTEM_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s", getSystemTag(TAG_SYSTEM_ID));
  }
}
