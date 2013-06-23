/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.common.logging.common;

import com.continuuity.common.logging.LoggingContext;

/**
 * Defines format for log message text. We need it to be able to later parse log messages (incl.
 * parsing multi-line messages, parsing tags etc.)
 */
public interface LogMessageFormat {
  String format(String message, String[] traceLines, LoggingContext context, String[] userTags);
}
