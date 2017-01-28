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
 * Sample plugable rolling log appender implementation
 */
public class RollingLocationLogAppender extends FileAppender<ILoggingEvent> implements Flushable {
  private static final Logger LOG = LoggerFactory.getLogger(RollingLocationLogAppender.class);

  // TODO LocationFactory should be passed from the log pipeline
  @Inject
  protected LocationFactory locationFactory;

  private SizeBasedTriggeringPolicy triggeringPolicy;
  private FixedWindowRollingPolicy rollingPolicy;
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
    }
    // TODO remove if condition after removing Guice injection
    if (locationFactory != null) {
      this.locationManager = new LocationManager(locationFactory);
      locationManager.pluginDirectoryLocation = locationFactory.create("plugins/logs");
      started = true;
    }
  }

  @Override
  public void stop() {
    closeEncoder();
    locationManager.clearAllLocations();
    LOG.info("Stopping custom appender");
    this.started = false;
  }

  @Override
  public void doAppend (ILoggingEvent eventObject) {
    try {
      // TODO only for testing remove 'if' after removing Guice injection
      if (locationFactory != null) {
        locationManager.pluginDirectoryLocation = locationFactory.create("plugins/logs");
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

      for (Location location : locationManager.activeFilesToLocation.values()) {
        triggeringPolicy.updateActiveFileLocation(location);
        // Since TriggeringPolicy#isTriggeringEvent method takes File in the api, we provide dummy file to our
        // triggeringPolicy implementation
        if (triggeringPolicy.isTriggeringEvent(new File("dummy"), eventObject)) {
          try {
            LocationIdentifier logLocationIdentifier = locationManager.getLocationIdentifier
              (eventObject.getMDCPropertyMap());
            rollingPolicy.updateActiveFileLocation(locationManager.activeFilesToLocation.get(logLocationIdentifier));
            rollingPolicy.rollover();
            // remove rolled over file from cache so that new file will get created
            locationManager.activeFiles.remove(logLocationIdentifier);
            locationManager.activeFilesToLocation.remove(logLocationIdentifier);
          } catch (IOException e) {
            throw new LogbackException("Exception while rolling over", e);
          } catch (RolloverFailure e) {
            // we do not want to stop processing because roll over failed so catch it and process the event
            addStatus(new WarnStatus("Attempt to rollover failed for appender [" + name + "].", this));
            LOG.info("Attempt to rollover failed fro appender [" + name + "].", this);
          }
        }
      }

      String namespaceId = eventObject.getMDCPropertyMap().get(LocationManager.TAG_NAMESPACE_ID);
      if (!namespaceId.equals(NamespaceId.SYSTEM.getEntityName())) {
        LocationIdentifier logLocationIdentifier = locationManager.getLocationIdentifier(eventObject
                                                                                            .getMDCPropertyMap());
        OutputStream locationOutputStream = locationManager.getLocationOutputStream(logLocationIdentifier);
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
  public void flush() throws IOException {

  }

  public RollingPolicy getRollingPolicy() {
    return rollingPolicy;
  }

  public TriggeringPolicy<ILoggingEvent> getTriggeringPolicy() {
    return triggeringPolicy;
  }

  /**
   * Sets the rolling policy. In case the 'policy' argument also implements
   * {@link TriggeringPolicy}, then the triggering policy for this appender is
   * automatically set to be the policy argument.
   *
   * @param policy
   */
  @SuppressWarnings("unchecked")
  public void setRollingPolicy(FixedWindowRollingPolicy policy) {
    rollingPolicy = policy;
  }

  public void setTriggeringPolicy(SizeBasedTriggeringPolicy policy) {
    triggeringPolicy = policy;
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
}
