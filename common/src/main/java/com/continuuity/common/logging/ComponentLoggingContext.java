package com.continuuity.common.logging;

/**
 * Component Logging Context.
 */
public class ComponentLoggingContext extends SystemLoggingContext {
  public static final String TAG_COMPONENT_ID = ".componentId";

  /**
   * Constructs ComponentLoggingContext.
   * @param systemId system id
   * @param componentId component id
   */
  public ComponentLoggingContext(final String systemId, final String componentId) {
    super(systemId);
    setSystemTag(TAG_COMPONENT_ID, componentId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_COMPONENT_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s/%s", super.getLogPathFragment(), getSystemTag(TAG_COMPONENT_ID));
  }
}
