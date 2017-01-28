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

import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.rolling.RolloverFailure;
import ch.qos.logback.core.rolling.helper.CompressionMode;
import ch.qos.logback.core.rolling.helper.FileFilterUtil;
import ch.qos.logback.core.rolling.helper.FileNamePattern;
import ch.qos.logback.core.rolling.helper.IntegerTokenConverter;
import org.apache.twill.filesystem.Location;

import java.io.IOException;

/**
 * When rolling over, renames files according to a fixed window algorithm.
 */
public class FixedWindowRollingPolicy extends LocationRollingPolicyBase {
  private static final String FNP_NOT_SET =
    "The \"FileNamePattern\" property must be set before using FixedWindowRollingPolicy. ";
  private int maxIndex;
  private int minIndex;
  // TODO Support compression
//  private Compressor compressor;

  private static final String ZIP_ENTRY_DATE_PATTERN = "yyyy-MM-dd_HHmm";

  /**
   * It's almost always a bad idea to have a large window size, say over 12.
   */
  private static final int MAX_WINDOW_SIZE = 12;

  public FixedWindowRollingPolicy() {
    minIndex = 1;
    maxIndex = 7;
  }

  @Override
  public void start() {
    if (fileNamePatternStr != null) {
      fileNamePattern = new FileNamePattern(fileNamePatternStr, this.context);
      determineCompressionMode();
    } else {
      addError(FNP_NOT_SET);
      addError(CoreConstants.SEE_FNP_NOT_SET);
      throw new IllegalStateException(FNP_NOT_SET + CoreConstants.SEE_FNP_NOT_SET);
    }

    if (maxIndex < minIndex) {
      addWarn("MaxIndex (" + maxIndex + ") cannot be smaller than MinIndex ("
                + minIndex + ").");
      addWarn("Setting maxIndex to equal minIndex.");
      maxIndex = minIndex;
    }

    if ((maxIndex - minIndex) > MAX_WINDOW_SIZE) {
      addWarn("Large window sizes are not allowed.");
      maxIndex = minIndex + MAX_WINDOW_SIZE;
      addWarn("MaxIndex reduced to " + maxIndex);
    }

    IntegerTokenConverter itc = fileNamePattern.getIntegerTokenConverter();

    if (itc == null) {
      throw new IllegalStateException("FileNamePattern ["
                                        + fileNamePattern.getPattern()
                                        + "] does not contain a valid IntegerToken");
    }

    if (compressionMode == CompressionMode.ZIP) {
      String zipEntryFileNamePatternStr = transformFileNamePatternFromInt2Date(fileNamePatternStr);
      zipEntryFileNamePattern = new FileNamePattern(zipEntryFileNamePatternStr, context);
    }

    super.start();
  }

  private String transformFileNamePatternFromInt2Date(String fileNamePatternStr) {
    String slashified = FileFilterUtil.slashify(fileNamePatternStr);
    String stemOfFileNamePattern = FileFilterUtil.afterLastSlash(slashified);
    return stemOfFileNamePattern.replace("%i", "%d{" + ZIP_ENTRY_DATE_PATTERN + "}");
  }

  /**
   * Given the FileNamePattern string, this method determines the compression
   * mode depending on last letters of the fileNamePatternStr. Patterns ending
   * with .gz imply GZIP compression, endings with '.zip' imply ZIP compression.
   * Otherwise and by default, there is no compression.
   */
  @Override
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

  @Override
  public void rollover() throws RolloverFailure {

    // Inside this method it is guaranteed that the hereto active log file is
    // closed.
    // If maxIndex <= 0, then there is no file renaming to be done.
    if (maxIndex >= 0) {
      try {
        // Delete the oldest file, to keep Windows happy.
        String fileName = fileNamePattern.convertInt(maxIndex);
        Location deleteLocation = activeParentLocation.append(fileName);
        if (deleteLocation.exists()) {
          deleteLocation.delete();
        }

        // Map {(maxIndex - 1), ..., minIndex} to {maxIndex, ..., minIndex+1}
        for (int i = maxIndex - 1; i >= minIndex; i--) {
          String toRenameStr = fileNamePattern.convertInt(i);
          Location toRename = activeParentLocation.append(toRenameStr);
          // no point in trying to rename an non existent file
          if (toRename.exists()) {
            Location newName = activeParentLocation.append(fileNamePattern.convertInt(i + 1));
            toRename.renameTo(newName);
          } else {
            addInfo("Skipping roll-over for inexistent file " + toRenameStr);
          }
        }
      } catch (IOException e) {
        throw new RolloverFailure(e.getMessage());
      }

      // move active file name to min
      switch (compressionMode) {
        case NONE:
          try {
            activeFileLocation.renameTo(activeParentLocation.append(fileNamePattern.convertInt(minIndex)));
          } catch (IOException e) {
            throw new RolloverFailure(String.format("Exception while renaming file: %s, %s",
                                                    activeFileLocation.getName(), e.getMessage()));
          }
          break;
        case GZ:
          // not supported
          break;
        case ZIP:
          // not supported
          break;
      }
    }
  }

  /**
   * Return the value of the parent's RawFile property.
   */
  public String getActiveFileName() {
    return activeFileLocation.getName();
  }

  public int getMaxIndex() {
    return maxIndex;
  }

  public int getMinIndex() {
    return minIndex;
  }

  public void setMaxIndex(int maxIndex) {
    this.maxIndex = maxIndex;
  }

  public void setMinIndex(int minIndex) {
    this.minIndex = minIndex;
  }
}
