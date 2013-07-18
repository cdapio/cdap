/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.serialize;

import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;

import java.util.Arrays;

/**
 * Class used as helper during serialization of IThrowableProxy.
 */
public final class ThrowableProxyImpl implements IThrowableProxy {
  private final IThrowableProxy cause;
  private final String className;
  private final int commonFrames;
  private final String message;
  private final StackTraceElementProxy[] stackTraceElementProxyArray;
  private final IThrowableProxy[] suppressed;

  public ThrowableProxyImpl(IThrowableProxy cause, String className, int commonFrames, String message,
                            StackTraceElementProxy[] stackTraceElementProxyArray, IThrowableProxy[] suppressed) {
    this.cause = cause;
    this.className = className;
    this.commonFrames = commonFrames;
    this.message = message;
    this.stackTraceElementProxyArray = stackTraceElementProxyArray;
    this.suppressed = suppressed;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public StackTraceElementProxy[] getStackTraceElementProxyArray() {
    return stackTraceElementProxyArray;
  }

  @Override
  public int getCommonFrames() {
    return commonFrames;
  }

  @Override
  public IThrowableProxy getCause() {
    return cause;
  }

  @Override
  public IThrowableProxy[] getSuppressed() {
    return suppressed;
  }

  @Override
  public String toString() {
    return "ThrowableProxyImpl{" +
      "cause=" + cause +
      ", className='" + className + '\'' +
      ", commonFrames=" + commonFrames +
      ", message='" + message + '\'' +
      ", stackTraceElementProxyArray=" +
      (stackTraceElementProxyArray == null ? null : Arrays.asList(stackTraceElementProxyArray)) +
      ", suppressed=" + (suppressed == null ? null : Arrays.asList(suppressed)) +
      '}';
  }
}
