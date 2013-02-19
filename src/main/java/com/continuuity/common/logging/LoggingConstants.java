/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.common.logging;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Common logging constants
 */
public final class LoggingConstants {
  /**
   * This marker should be used for logs that are emitted within logging context which we don't want to show to user.
   * Since every log message emitted thru slf4j api in place where logging context is available will be injected with
   * info from that context and propagated to the common store of the log messages shown to user, we have to mark it
   * with this marker to avoid log message to be shown to user.
   */
  public static final Marker SYSTEM_MARKER = MarkerFactory.getMarker("-SYSTEM-");
}
