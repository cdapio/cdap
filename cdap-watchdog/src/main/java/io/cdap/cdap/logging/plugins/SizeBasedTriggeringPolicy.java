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

/**
 * SizeBasedTriggeringPolicy looks at size of the file being currently written
 * to. If it grows bigger than the specified size, the {@link RollingLocationLogAppender} using the
 * SizeBasedTriggeringPolicy rolls the file and creates a new one.
 */
public class SizeBasedTriggeringPolicy extends LocationTriggeringPolicyBase {
  /**
   * The default maximum file size.
   */
  private static final long DEFAULT_MAX_FILE_SIZE = 10 * 1024 * 1024; // 10 MB

  private String maxFileSizeAsString = Long.toString(DEFAULT_MAX_FILE_SIZE);
  private FileSize maxFileSize;

  public SizeBasedTriggeringPolicy() {
  }

  public SizeBasedTriggeringPolicy(final String maxFileSize) {
    setMaxFileSize(maxFileSize);
  }

  @Override
  public boolean isTriggeringEvent(final ILoggingEvent event) throws LogbackException {
    return (getActiveLocationSize() >= maxFileSize.getSize());
  }

  public String getMaxFileSize() {
    return maxFileSizeAsString;
  }

  public void setMaxFileSize(String maxFileSize) {
    this.maxFileSizeAsString = maxFileSize;
    this.maxFileSize = FileSize.valueOf(maxFileSize);
  }
}
