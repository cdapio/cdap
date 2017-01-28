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


/**
 * Location rolling policy base which provides
 */
public abstract class LocationRollingPolicyBase extends ContextAwareBase implements LocationRollingPolicy {

  protected CompressionMode compressionMode = CompressionMode.NONE;
  protected FileNamePattern fileNamePattern;
  // fileNamePatternStr is always slashified, see setter
  protected String fileNamePatternStr;
  private FileAppender parent;
  // use to name files within zip file, i.e. the zipEntry
  FileNamePattern zipEntryFileNamePattern;

  protected Location activeParentLocation;
  protected Location activeFileLocation;
  private boolean started;

  /**
   * Given the FileNamePattern string, this method determines the compression
   * mode depending on last letters of the fileNamePatternStr. Patterns ending
   * with .gz imply GZIP compression, endings with '.zip' imply ZIP compression.
   * Otherwise and by default, there is no compression.
   */
  protected void determineCompressionMode() {
    if (fileNamePatternStr.endsWith(".gz")) {
      addInfo("Will use gz compression");
      compressionMode = CompressionMode.GZ;
    } else if (fileNamePatternStr.endsWith(".zip")) {
      addInfo("Will use zip compression");
      compressionMode = CompressionMode.ZIP;
    } else {
      addInfo("No compression will be used");
      compressionMode = CompressionMode.NONE;
    }
  }

  public void setFileNamePattern(String fnp) {
    fileNamePatternStr = fnp;
  }

  public String getFileNamePattern() {
    return fileNamePatternStr;
  }

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

  public void setLocation(Location parentDirLocation, Location activeFileLocation) {
    this.activeParentLocation = parentDirLocation;
    this.activeFileLocation = activeFileLocation;
  }
}
