/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.log;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 *
 */
public class SystemLoggerConstants {
  public static final Marker USER = MarkerFactory.getMarker("USER");
  public static final Marker SYSTEM = MarkerFactory.getMarker("SYSTEM");
}
