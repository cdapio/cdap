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
import ch.qos.logback.core.rolling.helper.FileNamePattern;
import ch.qos.logback.core.rolling.helper.IntegerTokenConverter;
import co.cask.cdap.api.logging.AppenderContext;
import co.cask.cdap.common.conf.Constants;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import javax.annotation.Nullable;

/**
 * When rolling over, renames files according to a fixed window algorithm.
 */
public class FixedWindowRollingPolicy extends LocationRollingPolicyBase {
  private static final Logger LOG = LoggerFactory.getLogger(FixedWindowRollingPolicy.class);

  private static final String FNP_NOT_SET =
    "The \"FileNamePattern\" property must be set before using FixedWindowRollingPolicy. ";

  // TODO CDAP-8369 - Support compression
  private int minIndex;
  private int maxIndex;

  private int processedIndex;

  /**
   * It's almost always a bad idea to have a large window size, say over 20.
   */
  private static final int MAX_WINDOW_SIZE = 20;

  public FixedWindowRollingPolicy() {
    minIndex = 1;
    maxIndex = 7;
  }

  @Override
  public void start() {
    if (fileNamePatternStr != null) {
      if (context instanceof AppenderContext) {
        AppenderContext context = (AppenderContext) this.context;
        fileNamePatternStr = fileNamePatternStr.replace("instanceId", Integer.toString(context.getInstanceId()));
      } else if (!Boolean.TRUE.equals(context.getObject(Constants.Logging.PIPELINE_VALIDATION))) {
        throw new IllegalStateException("Expected logger context instance of " + AppenderContext.class.getName() +
                                          " but got " + context.getClass().getName());
      }

      fileNamePattern = new FileNamePattern(fileNamePatternStr, this.context);
    } else {
      LOG.error(FNP_NOT_SET);
      throw new IllegalStateException(FNP_NOT_SET + CoreConstants.SEE_FNP_NOT_SET);
    }

    if (maxIndex < minIndex) {
      LOG.warn("MaxIndex {} cannot be smaller than MinIndex {}.", maxIndex, minIndex);
      maxIndex = minIndex;
    }

    if ((maxIndex - minIndex) > MAX_WINDOW_SIZE) {
      LOG.warn("Large window sizes are not allowed.");
      maxIndex = minIndex + MAX_WINDOW_SIZE;
      LOG.warn("MaxIndex reduced to " + maxIndex);
    }

    IntegerTokenConverter itc = fileNamePattern.getIntegerTokenConverter();

    if (itc == null) {
      throw new IllegalStateException("FileNamePattern ["
                                        + fileNamePattern.getPattern()
                                        + "] does not contain a valid IntegerToken");
    }
    processedIndex = maxIndex;
    super.start();
  }

  @Override
  public void rollover() throws RolloverFailure {
    Location parentLocation = getParent(activeFileLocation);

    if (parentLocation == null) {
      return;
    }

    // If maxIndex <= 0, then there is no file renaming to be done.
    if (maxIndex >= 0) {
      try {
        // close outputstream of active location
        closeable.close();

        String fileName = fileNamePattern.convertInt(maxIndex);
        // Delete the oldest file. If processedIndex is not pointing to maxIndex, that means there was some
        // exception while doing rename of files in previous rollover. So do not delete the existing file with
        // maximum index otherwise we will end up deleting rolled over file
        if (processedIndex == maxIndex) {
          Location deleteLocation = parentLocation.append(fileName);
          // no need to proceed further if we are not able to delete location so throw exception
          if (deleteLocation.exists() && !deleteLocation.delete()) {
            LOG.warn("Failed to delete location: {}", deleteLocation.toURI().toString());
            throw new RolloverFailure(String.format("Not able to delete file: %s", deleteLocation.toURI().toString()));
          }
          processedIndex--;
        }

        for (int i = processedIndex; i >= minIndex; i--, processedIndex--) {
          String toRenameStr = fileNamePattern.convertInt(i);
          Location toRename = parentLocation.append(toRenameStr);

          // no point in trying to rename an non existent file
          if (toRename.exists()) {
            Location newName = parentLocation.append(fileNamePattern.convertInt(i + 1));
            // throw exception if rename fails, so that in next iteration of rollover, it will be retried
            if (toRename.renameTo(newName) == null) {
              LOG.warn("Failed to rename {} to {}", toRename.toURI().toString(), newName.toURI().toString());
              throw new RolloverFailure(String.format("Failed to rename %s to %s", toRename.toURI().toString(),
                                                      newName.toURI().toString()));
            }
          } else {
            LOG.trace("Skipping roll-over for inexistent file {}", toRename.toURI().toString());
          }
        }

        if (activeFileLocation.renameTo(parentLocation.append(fileNamePattern.convertInt(minIndex))) == null) {
          LOG.warn("Failed to rename location: {}", activeFileLocation.toURI().toString());
          throw new RolloverFailure(String.format("Not able to rename file: %s",
                                                  activeFileLocation.toURI().toString()));
        }

        // reset max processed index after rename of active location has been processed successfully
        processedIndex = maxIndex;
      } catch (IOException e) {
        RolloverFailure f = new RolloverFailure(e.getMessage());
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  /**
   * Return the value of the parent's RawFile property.
   */
  @Override
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

  /**
   * Creates a {@link Location} instance which represents the parent of the given location.
   *
   * @param location location to extra parent from.
   * @return an instance representing the parent location or {@code null} if there is no parent.
   */
  @Nullable
  private static Location getParent(Location location) {
    URI source = location.toURI();

    // If it is root, return null
    if ("/".equals(source.getPath())) {
      return null;
    }

    URI resolvedParent = URI.create(source.toString() + "/..").normalize();
    // NOTE: if there is a trailing slash at the end, rename(), getName() and other operations on file
    // does not work in MapR. so we remove the trailing slash (if any) at the end.
    if (resolvedParent.toString().endsWith("/")) {
      String parent = resolvedParent.toString();
      resolvedParent = URI.create(parent.substring(0, parent.length() - 1));
    }
    return location.getLocationFactory().create(resolvedParent);
  }
}
