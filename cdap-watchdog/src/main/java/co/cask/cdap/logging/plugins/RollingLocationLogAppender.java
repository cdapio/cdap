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
import ch.qos.logback.core.rolling.RollingPolicy;
import ch.qos.logback.core.rolling.TriggeringPolicy;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Sample plugable rolling log appender implementation
 */
public class RollingLocationLogAppender extends LocationLogAppender {
  private static final Logger LOG = LoggerFactory.getLogger(RollingLocationLogAppender.class);

  private SizeBasedTriggeringPolicy triggeringPolicy;
  private FixedWindowRollingPolicy rollingPolicy;

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
    super.start();
  }

  @Override
  public void stop() {
    super.stop();
  }

  @Override
  public void doAppend (ILoggingEvent eventObject) {
    for (Location location : activeFilesToLocation.values()) {
      triggeringPolicy.updateActiveFileLocation(location);
      // Since TriggeringPolicy#isTriggeringEvent method takes File in the api, we provide dummy file to our
      // triggeringPolicy implementation
      if (triggeringPolicy.isTriggeringEvent(new File("dummy"), eventObject)) {
        try {
          LocationIdentifier logLocationIdentifier = getLocationIdentifier(eventObject.getMDCPropertyMap());
          rollover(logLocationIdentifier);
        } catch (IOException e) {

        }
      }
    }

    super.doAppend(eventObject);
  }

  @Override
  public void flush() throws IOException {

  }

  /**
   * Implemented by delegating most of the rollover work to a rolling policy.
   */
  public void rollover(LocationIdentifier locationIdentifier) {
    this.closeOutputStream();
    if (!activeFiles.containsKey(locationIdentifier)) {
      return;
    }

    Location location = activeFilesToLocation.get(locationIdentifier);
    activeFiles.remove(locationIdentifier);
    activeFilesToLocation.remove(locationIdentifier);

    try {
      Location contextLocation = pluginDirectoryLocation.append(locationIdentifier.getNamespaceId())
        .append(locationIdentifier.getApplicationId());

      // rename rolled over file
      String renamedFileName = location.getName() + "-" +  System.currentTimeMillis();
      Location renamedLocation = contextLocation.append(renamedFileName);
      location.renameTo(renamedLocation);

      // create new file
      int sequenceId = contextLocation.list().size();
      String newFilname = String.format("%s.log", sequenceId);
      Location newLocation = contextLocation.append(newFilname);

      activeFiles.put(locationIdentifier, newLocation.getOutputStream());
      activeFilesToLocation.put(locationIdentifier, newLocation);
    } catch (IOException e) {

    }

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
}
