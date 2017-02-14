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
import ch.qos.logback.core.rolling.RollingPolicy;
import ch.qos.logback.core.rolling.RolloverFailure;
import ch.qos.logback.core.rolling.TriggeringPolicy;
import ch.qos.logback.core.spi.FilterReply;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.logging.framework.AppenderContext;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Rolling Appender for {@link Location}
 */
public class RollingLocationLogAppender extends FileAppender<ILoggingEvent> implements Flushable {
  private static final Logger LOG = LoggerFactory.getLogger(RollingLocationLogAppender.class);

  private TriggeringPolicy<ILoggingEvent> triggeringPolicy;
  private RollingPolicy rollingPolicy;

  // used as cache to avoid type casting of triggeringPolicy on every event
  private LocationTriggeringPolicy locationTriggeringPolicy;
  // used as cache to avoid type casting of triggeringPolicy on every event
  private LocationRollingPolicy locationRollingPolicy;

  // log file path will be created by this appender as: <basePath>/namespaceId/applicationId/<filePath>
  private String basePath;
  private String filePath;
  private String filePermissions;
  private String dirPermissions;
  private LocationManager locationManager;

  public RollingLocationLogAppender() {
    setName(getClass().getName());
  }

  @Override
  public void start() {
    // These should all passed. The settings are from the custom-log-pipeline.xml and
    // the context must be AppenderContext
    Preconditions.checkState(basePath != null, "Property basePath must be base directory.");
    Preconditions.checkState(filePath != null, "Property filePath must be filePath along with filename.");
    Preconditions.checkState(triggeringPolicy != null, "Property triggeringPolicy must be specified.");
    Preconditions.checkState(rollingPolicy != null, "Property rollingPolicy must be specified");
    Preconditions.checkState(encoder != null, "Property encoder must be specified.");
    Preconditions.checkState(dirPermissions != null, "Property dirPermissions cannot be null");
    Preconditions.checkState(filePermissions != null, "Property filePermissions cannot be null");

    if (context instanceof AppenderContext) {
      AppenderContext context = (AppenderContext) this.context;
      locationManager = new LocationManager(context.getLocationFactory(), basePath, dirPermissions, filePermissions);
      filePath = filePath.replace("instanceId", Integer.toString(context.getInstanceId()));
    } else if (!Boolean.TRUE.equals(context.getObject(Constants.Logging.PIPELINE_VALIDATION))) {
      throw new IllegalStateException("Expected logger context instance of " + AppenderContext.class.getName() +
                                        " but got " + context.getClass().getName());
    }

    started = true;
  }

  @Override
  public void doAppend(ILoggingEvent eventObject) throws LogbackException {
    try {
      // logic from AppenderBase
      if (!this.started) {
        LOG.warn("Attempted to append to non started appender {}", this.getName());
        return;
      }

      // logic from AppenderBase
      if (getFilterChainDecision(eventObject) == FilterReply.DENY) {
        return;
      }

      String namespaceId = eventObject.getMDCPropertyMap().get(LocationManager.TAG_NAMESPACE_ID);

      if (namespaceId != null && !namespaceId.equals(NamespaceId.SYSTEM.getNamespace())) {
        LocationIdentifier logLocationIdentifier = locationManager.getLocationIdentifier(eventObject
                                                                                           .getMDCPropertyMap());
        rollover(logLocationIdentifier, eventObject);
        OutputStream locationOutputStream = locationManager.getLocationOutputStream(logLocationIdentifier, filePath);
        setOutputStream(locationOutputStream);
        writeOut(eventObject);
      }
    } catch (IllegalArgumentException iae) {
      // this shouldn't happen
      LOG.error("Unrecognized context ", iae);
    } catch (IOException ioe) {
      throw new LogbackException("Exception while appending event. ", ioe);
    }
  }

  private void rollover(final LocationIdentifier logLocationIdentifier, ILoggingEvent logEvent) {
    if (!locationManager.getActiveLocations().containsKey(logLocationIdentifier)) {
      LOG.info("Decided not to roll over for first event");
      return;
    }
    final LocationOutputStream locationOutputStream = locationManager.getActiveLocations().get(logLocationIdentifier);

    if (triggeringPolicy instanceof LocationTriggeringPolicy) {
      // no need to type cast on every event
      if (locationTriggeringPolicy == null) {
        locationTriggeringPolicy = ((LocationTriggeringPolicy) triggeringPolicy);
      }

      locationTriggeringPolicy.setLocation(locationOutputStream.getLocation());
      // set number of bytes written to locationOutputStream, we need to do this because HDFS does not provide
      // correct size of the file
      locationTriggeringPolicy.setActiveLocationSize(locationOutputStream.getNumOfBytes());

      if (locationTriggeringPolicy.isTriggeringEvent(logEvent)) {
        try {
          if (rollingPolicy instanceof LocationRollingPolicy) {
            // no need to type cast on every event
            if (locationRollingPolicy == null) {
              locationRollingPolicy = ((LocationRollingPolicy) rollingPolicy);
            }

            locationRollingPolicy.setLocation(locationOutputStream.getLocation(), new Closeable() {
              @Override
              public void close() throws IOException {
                locationOutputStream.close();
                locationManager.getActiveLocations().remove(logLocationIdentifier);
              }
            });

            locationRollingPolicy.rollover();
          }
        } catch (RolloverFailure e) {
          // we do not want to stop processing because roll over failed. so catch it and process the event
          LOG.warn("Attempt to rollover failed for appender {}.", name);
        }
      }
    }
  }

  @Override
  public void flush() throws IOException {
    locationManager.flush();
  }

  @Override
  public void stop() {
    try {
      LOG.info("Stopping appender {}", this.name);
      locationManager.close();
      if (encoder != null) {
        encoder.close();
      }
    } catch (IOException ioe) {
      LOG.error("Failed to write footer for appender named {}", this.getName(), ioe);
    } finally {
      this.started = false;
    }
  }

  // override this method to setOutputStream for every event since this appender is appending to multiple files
  @Override
  public void setOutputStream(OutputStream outputStream) {
    if (encoder == null) {
      LOG.warn("Encoder has not been set. Cannot invoke its init method.");
      return;
    }

    try {
      encoder.init(outputStream);
    } catch (IOException ioe) {
      this.started = false;
      LOG.error("Failed to initialize encoder for appender named {}", name, ioe);
    }
  }

  // Since this appender does not support prudent mode, we override writeOut method from FileAppender
  @Override
  protected void writeOut(ILoggingEvent event) throws IOException {
    this.encoder.doEncode(event);
  }

  @VisibleForTesting
  LocationManager getLocationManager() {
    return locationManager;
  }

  public void setRollingPolicy(RollingPolicy policy) {
    rollingPolicy = policy;
  }

  public void setTriggeringPolicy(TriggeringPolicy<ILoggingEvent> policy) {
    triggeringPolicy = policy;
  }

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  public String getFilePermissions() {
    return filePermissions;
  }

  public void setFilePermissions(String filePermissions) {
    this.filePermissions = filePermissions;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public String getDirPermissions() {
    return dirPermissions;
  }

  public void setDirPermissions(String dirPermissions) {
    this.dirPermissions = dirPermissions;
  }
}
