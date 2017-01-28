/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.plugins;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.util.FileSize;
import ch.qos.logback.core.util.InvocationGate;

import java.io.File;
import java.io.IOException;

/**
 * SizeBasedTriggeringPolicy looks at size of the file being currently written
 * to. If it grows bigger than the specified size, the {@link RollingLocationLogAppender} using the
 * SizeBasedTriggeringPolicy rolls the file and creates a new one.
 */
public class SizeBasedTriggeringPolicy extends LocationTriggeringPolicyBase {
  public static final String SEE_SIZE_FORMAT = "http://logback.qos.ch/codes.html#sbtp_size_format";
  /**
   * The default maximum file size.
   */
  public static final long DEFAULT_MAX_FILE_SIZE = 10 * 1024 * 1024; // 10 MB

  private String maxFileSizeAsString = Long.toString(DEFAULT_MAX_FILE_SIZE);
  private FileSize maxFileSize;

  public SizeBasedTriggeringPolicy() {
  }

  public SizeBasedTriggeringPolicy(final String maxFileSize) {
    setMaxFileSize(maxFileSize);
  }

  private InvocationGate invocationGate = new InvocationGate();

  @Override
  public boolean isTriggeringEvent(final File activeFile, final ILoggingEvent event) throws LogbackException {
    if (invocationGate.skipFurtherWork()) {
      return false;
    }

    long now = System.currentTimeMillis();
    invocationGate.updateMaskIfNecessary(now);

    try {
      return (activeLocation.length() >= maxFileSize.getSize());
    } catch (IOException e) {
      throw new LogbackException("Exception while accessing length for triggering roll over", e);
    }
  }

  public String getMaxFileSize() {
    return maxFileSizeAsString;
  }

  public void setMaxFileSize(String maxFileSize) {
    this.maxFileSizeAsString = maxFileSize;
    this.maxFileSize = FileSize.valueOf(maxFileSize);
  }

  long toFileSize(String value) {
    if (value == null) {
      return DEFAULT_MAX_FILE_SIZE;
    }

    String s = value.trim().toUpperCase();
    long multiplier = 1;
    int index;

    if ((index = s.indexOf("KB")) != -1) {
      multiplier = 1024;
      s = s.substring(0, index);
    } else if ((index = s.indexOf("MB")) != -1) {
      multiplier = 1024 * 1024;
      s = s.substring(0, index);
    } else if ((index = s.indexOf("GB")) != -1) {
      multiplier = 1024 * 1024 * 1024;
      s = s.substring(0, index);
    }
    if (s != null) {
      try {
        return Long.valueOf(s).longValue() * multiplier;
      } catch (NumberFormatException e) {
        addError("[" + s + "] is not in proper int format. Please refer to "
                   + SEE_SIZE_FORMAT);
        addError("[" + value + "] not in expected format.", e);
      }
    }
    return DEFAULT_MAX_FILE_SIZE;
  }

}
