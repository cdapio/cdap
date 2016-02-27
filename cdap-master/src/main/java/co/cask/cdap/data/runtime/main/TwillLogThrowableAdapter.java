/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data.runtime.main;

import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import org.apache.twill.api.logging.LogThrowable;

/**
 *
 */
public class TwillLogThrowableAdapter implements IThrowableProxy {

  private final LogThrowable throwable;

  public TwillLogThrowableAdapter(LogThrowable throwable) {
    this.throwable = throwable;
  }

  @Override
  public String getMessage() {
    return throwable.getMessage();
  }

  @Override
  public String getClassName() {
    return throwable.getClassName();
  }

  @Override
  public StackTraceElementProxy[] getStackTraceElementProxyArray() {
    StackTraceElementProxy[] result = new StackTraceElementProxy[throwable.getStackTraces().length];
    for (int i = 0; i < throwable.getStackTraces().length; i++) {
      StackTraceElement original = throwable.getStackTraces()[i];
      result[i] = new StackTraceElementProxy(original);
    }
    return result;
  }

  @Override
  public int getCommonFrames() {
    return 0;
  }

  @Override
  public IThrowableProxy getCause() {
    if (throwable.getCause() == null) {
      return null;
    }

    return new TwillLogThrowableAdapter(throwable.getCause());
  }

  @Override
  public IThrowableProxy[] getSuppressed() {
    return new IThrowableProxy[0];
  }
}
