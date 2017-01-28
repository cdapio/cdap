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

import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.rolling.helper.CompressionMode;
import ch.qos.logback.core.rolling.helper.FileNamePattern;
import ch.qos.logback.core.spi.ContextAwareBase;
import org.apache.twill.filesystem.Location;

import java.io.Closeable;


/**
 * Location rolling policy base which provides
 */
public abstract class LocationRollingPolicyBase extends ContextAwareBase implements LocationRollingPolicy {

  private CompressionMode compressionMode = CompressionMode.NONE;
  protected FileNamePattern fileNamePattern;
  // fileNamePatternStr is always slashified, see setter
  protected String fileNamePatternStr;
  private FileAppender parent;


  protected Location activeFileLocation;
  protected Closeable closeable;
  private boolean started;

  //logback calls these methods, so do not remove
  public void setFileNamePattern(String fnp) {
    fileNamePatternStr = fnp;
  }

  public String getFileNamePattern() {
    return fileNamePatternStr;
  }

  /**
   * This interface is part of {@link ch.qos.logback.core.rolling.RollingPolicy}
   * @return
   */
  public CompressionMode getCompressionMode() {
    return compressionMode;
  }

  public boolean isStarted() {
    return started;
  }

  public void start() {
    started = true;
  }

  public void stop() {
    started = false;
  }

  public void setParent(FileAppender appender) {
    this.parent = appender;
  }

  public void setLocation(Location activeFileLocation, Closeable closeable) {
    this.activeFileLocation = activeFileLocation;
    this.closeable = closeable;
  }
}
