/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.logging.serialize;

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
