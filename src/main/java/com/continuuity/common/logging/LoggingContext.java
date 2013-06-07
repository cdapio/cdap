/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.common.logging;

import java.util.Collection;
import java.util.Map;

/**
 * Defines LoggingContext interface so that we have at least somewhat structured logging context data and know how to
 * inject it on the logs writing end.
 */
public interface LoggingContext {
  /**
   * @return collection of system tags associated with this logging context
   */
  Collection<SystemTag> getSystemTags();

  /**
   * @return Map of tag name to system tag associated with this logging context
   */
  Map<String, SystemTag> getSystemTagsMap();

  // hack hack hack: time constraints
  /**
   * @return log partition name that is used to group log messages into one storage partition on the back-end.
   * In current implementation you can only tail/search thru single log partition when retrieving logs.
   */
  String getLogPartition();

  /**
   * Defines the interface for the system tag associated with LoggingContext
   */
  interface SystemTag {
    /**
     * @return tag name
     */
    String getName();

    /**
     * @return tag value
     */
    String getValue();
  }
}
