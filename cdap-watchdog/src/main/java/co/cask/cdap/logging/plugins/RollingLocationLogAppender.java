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
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.RolloverFailure;
import ch.qos.logback.core.rolling.TriggeringPolicy;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.ErrorStatus;
import ch.qos.logback.core.status.WarnStatus;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Rolling Appender for {@link Location}
 */
public class RollingLocationLogAppender extends RollingFileAppender<ILoggingEvent> implements Flushable {
  private static final Logger LOG = LoggerFactory.getLogger(RollingLocationLogAppender.class);

  // TODO LocationFactory should be passed from Log framework
  @Inject
  protected LocationFactory locationFactory;

  private TriggeringPolicy<ILoggingEvent> triggeringPolicy;
  private RollingFileAppender<ILoggingEvent> rollingPolicy;

  private LocationManager locationManager;


  public RollingLocationLogAppender() {
    setName(getClass().getName());
  }

  @Override
  public void start() {
    if (triggeringPolicy == null) {
      addWarn("No TriggeringPolicy was set for the RollingFileAppender named "
                + getName());
      return;
    }

    if (rollingPolicy == null) {
      addError("No RollingPolicy was set for the RollingFileAppender named "
                 + getName());
      return;
    }

    // logic from OutputStreamAppender
    if (this.encoder == null) {
      addStatus(new ErrorStatus("No encoder set for the appender named \""
                                  + name + "\".", this));
      return;
    }

    // TODO remove if condition after removing Guice injection
    if (locationFactory != null) {
      this.locationManager = new LocationManager(locationFactory);
      started = true;
    }
  }

  @Override
  public void doAppend(ILoggingEvent eventObject) throws LogbackException {
    try {
      // TODO Remove 'if' after removing Guice injection
      if (locationFactory != null && this.locationManager == null) {
        this.locationManager = new LocationManager(locationFactory);
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

      String namespaceId = eventObject.getMDCPropertyMap().get(LocationManager.TAG_NAMESPACE_ID);
      if (!namespaceId.equals(NamespaceId.SYSTEM.getEntityName())) {
        LocationIdentifier logLocationIdentifier = locationManager.getLocationIdentifier(eventObject
                                                                                           .getMDCPropertyMap());
        rollover(logLocationIdentifier, eventObject);
        OutputStream locationOutputStream = locationManager.getLocationOutputStream(logLocationIdentifier, fileName);
        setOutputStream(locationOutputStream);
        writeOut(eventObject);
      }
    } catch (IllegalArgumentException iae) {
      // this shouldn't happen
      LOG.error("Unrecognized context ", iae);
    } catch (IOException ioe) {
      throw new LogbackException("Exception during append", ioe);
    }
  }

  private void rollover(LocationIdentifier logLocationIdentifier, ILoggingEvent eventObject) {
    if (!locationManager.getActiveFilesLocations().containsKey(logLocationIdentifier)) {
      return;
    }

    Location logFile = locationManager.getActiveFilesLocations().get(logLocationIdentifier);
    ((LocationTriggeringPolicy) triggeringPolicy).setLocation(logFile);

    // Since TriggeringPolicy#isTriggeringEvent method takes File in the api, we provide dummy file to our
    // triggeringPolicy implementation
    if (triggeringPolicy.isTriggeringEvent(new File("dummy"), eventObject)) {
      try {
        LocationRollingPolicy locationRollingPolicy = (LocationRollingPolicy) rollingPolicy);
        locationRollingPolicy.setLocation(locationManager.getLogLocation(logLocationIdentifier),
                                                            logFile);
        ((LocationRollingPolicy) rollingPolicy).rollover();

        // clear caches so new location gets created
        locationManager.closeOutputStream(locationManager.getActiveFiles().get(logLocationIdentifier));
        locationManager.getActiveFiles().remove(logLocationIdentifier);
        locationManager.getActiveFilesLocations().remove(logLocationIdentifier);

      } catch (IOException e) {
        throw new LogbackException("Exception while rolling over", e);
      } catch (RolloverFailure e) {
        // we do not want to stop processing because roll over failed so catch it and process the event
        addStatus(new WarnStatus("Attempt to rollover failed for appender [" + name + "].", this));
        LOG.info("Attempt to rollover failed fro appender [" + name + "].", this);
      }
    }
  }

  @Override
  public void flush() throws IOException {
    // no-op
  }

  @Override
  public void stop() {
    encoderClose();
    locationManager.close();
    LOG.info("Stopping appender {}", this.name);
    this.started = false;
  }


  public void setFile(String fileName) {
    this.fileName = fileName;
  }

  public String getFile() {
    return fileName;
  }

  protected void encoderInit(OutputStream outputStream) {
    if (encoder != null) {
      try {
        encoder.init(outputStream);
      } catch (IOException ioe) {
        this.started = false;
        addStatus(new ErrorStatus(
          "Failed to initialize encoder for appender named [" + name + "].", this, ioe));
      }
    }
  }

  protected void encoderClose() {
    if (encoder != null) {
      try {
        encoder.close();
      } catch (IOException ioe) {
        this.started = false;
        addStatus(new ErrorStatus("Failed to write footer for appender named [" + name + "].", this, ioe));
      }
    }
  }

  @Override
  public void setOutputStream(OutputStream outputStream) {
    if (encoder == null) {
      addWarn("Encoder has not been set. Cannot invoke its init method.");
      return;
    }

    encoderInit(outputStream);
  }

  public LocationManager getLocationManager() {
    return locationManager;
  }
}
