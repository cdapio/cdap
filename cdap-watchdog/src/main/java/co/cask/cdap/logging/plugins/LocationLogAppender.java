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
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.ErrorStatus;
import ch.qos.logback.core.status.WarnStatus;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Location appender which appends to a location similar to {@link ch.qos.logback.core.FileAppender}
 */
public class LocationLogAppender extends FileAppender<ILoggingEvent> implements Flushable {
  private static final Logger LOG = LoggerFactory.getLogger(LocationLogAppender.class);
  private static final String TAG_NAMESPACE_ID = ".namespaceId";
  private static final String TAG_APPLICATION_ID = ".applicationId";

  // TODO LocationFactory should be passed from the log pipeline
  @Inject
  protected LocationFactory locationFactory;

  protected Location pluginDirectoryLocation;

  protected Map<LocationIdentifier, OutputStream> activeFiles;
  protected Map<LocationIdentifier, Location> activeFilesToLocation;

  @Override
  public void start() {
    this.activeFiles = new HashMap<>();
    this.activeFilesToLocation = new HashMap<>();

    // logic from OutputStreamAppender
    if (this.encoder == null) {
      addStatus(new ErrorStatus("No encoder set for the appender named \""
                                  + name + "\".", this));
    }
    // TODO remove if condition after removing Guice injection
    if (locationFactory != null) {
      this.pluginDirectoryLocation = locationFactory.create("plugins/logs");
      started = true;
    }
  }

  @Override
  public void doAppend(ILoggingEvent eventObject) throws LogbackException {
    try {

      // TODO only for testing remove 'if' after removing Guice injection
      if (locationFactory != null) {
        pluginDirectoryLocation = locationFactory.create("plugins/logs");
        started = true;
      }

      // logic from AppenderBase
      if (!this.started) {
        addStatus(new WarnStatus(
          "Attempted to append to non started appender [" + name + "].",
          this));
        return;
      }

      // logic from AppenderBase
      if (getFilterChainDecision(eventObject) == FilterReply.DENY) {
        return;
      }

      String namespaceId = eventObject.getMDCPropertyMap().get(TAG_NAMESPACE_ID);
      if (!namespaceId.equals(NamespaceId.SYSTEM.getEntityName())) {
        LocationIdentifier logLocationIdentifier = getLocationIdentifier(eventObject.getMDCPropertyMap());
        OutputStream locationOutputStream = getLocationOutputStream(logLocationIdentifier);
        setOutputStream(locationOutputStream);
        writeOut(eventObject);
      }
    } catch (IllegalArgumentException iae) {
      // this shouldn't happen
      LOG.error("Unrecognized context ", iae);
    } catch (IOException ioe) {
      throw new LogbackException("Excepting during append", ioe);
    }
  }

  @Override
  protected void append(ILoggingEvent eventObject) {
    // no-op
  }

  @Override
  public void stop() {
    closeEncoder();
    Collection<OutputStream> locations = new ArrayList<>(activeFiles.values());
    activeFiles.clear();
    activeFilesToLocation.clear();

    for (OutputStream outputStream : locations) {
      Closeables.closeQuietly(outputStream);
    }
    LOG.info("Stopping custom appender");

    this.started = false;
  }

  protected LocationIdentifier getLocationIdentifier(Map<String, String> propertyMap) throws IllegalArgumentException,
    IOException {

    String namespaceId = propertyMap.get(TAG_NAMESPACE_ID);
    Preconditions.checkArgument(propertyMap.containsKey(TAG_APPLICATION_ID),
                                String.format("%s is expected but not found in the context %s",
                                              TAG_APPLICATION_ID, propertyMap));
    String application = propertyMap.get(TAG_APPLICATION_ID);

    return new LocationIdentifier(namespaceId, application);
  }

  protected OutputStream getLocationOutputStream(LocationIdentifier locationIdentifier) throws IOException {
    if (activeFiles.containsKey(locationIdentifier)) {
      return activeFiles.get(locationIdentifier);
    }

    ensureDirectoryCheck(pluginDirectoryLocation);
    Location contextLocation = pluginDirectoryLocation.append(locationIdentifier.getNamespaceId())
      .append(locationIdentifier.getApplicationId());
    ensureDirectoryCheck(contextLocation);

    int sequenceId = contextLocation.list().size();
    String fileName = String.format("%s.log", sequenceId);
    Location location = contextLocation.append(fileName);
    activeFiles.put(locationIdentifier, location.getOutputStream());
    activeFilesToLocation.put(locationIdentifier, location);
    return activeFiles.get(locationIdentifier);
  }

  protected void ensureDirectoryCheck(Location location) throws IOException {
    if (!location.exists()) {
      location.mkdirs();
    } else {
      if (!location.isDirectory()) {
        throw new IOException(
          String.format("File Exists at the logging location %s, Expected to be a directory", location));
      }
    }
  }

  protected void closeEncoder() {
    // no-op
    encoderClose();
  }

  protected void encoderInit(OutputStream outputStream) {
    if (encoder != null) {
      try {
        encoder.init(outputStream);
      } catch (IOException ioe) {
        this.started = false;
        addStatus(new ErrorStatus(
          "Failed to initialize encoder for appender named [" + name + "].",
          this, ioe));
      }
    }
  }

  protected void encoderClose() {
    if (encoder != null) {
      try {
        encoder.close();
      } catch (IOException ioe) {
        this.started = false;
        addStatus(new ErrorStatus("Failed to write footer for appender named ["
                                    + name + "].", this, ioe));
      }
    }
  }

  /**
   * <p>
   * Sets the @link OutputStream} where the log output will go. The specified
   * <code>OutputStream</code> must be opened by the user and be writable. The
   * <code>OutputStream</code> will be closed when the appender instance is
   * closed.
   *
   * @param outputStream An already opened OutputStream.
   */
  public void setOutputStream(OutputStream outputStream) {
    if (encoder == null) {
      addWarn("Encoder has not been set. Cannot invoke its init method.");
      return;
    }

    encoderInit(outputStream);
  }

  @Override
  public void flush() throws IOException {

  }
}
