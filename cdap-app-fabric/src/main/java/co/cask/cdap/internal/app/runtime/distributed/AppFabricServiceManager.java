/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.twill.MasterServiceManager;
import co.cask.cdap.common.zookeeper.election.LeaderElectionInfoService;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.SystemServiceLiveInfo;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.api.logging.LogEntry;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * App Fabric Service Management in Distributed Mode.
 */
public class AppFabricServiceManager implements MasterServiceManager {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricServiceManager.class);
  private static final long ELECTION_PARTICIPANTS_TIMEOUT_MS = 2000L;

  private final InetAddress hostname;
  private LeaderElectionInfoService electionInfoService;
  private final Map<String, LogEntry.Level> oldLogLevels;

  @Inject
  public AppFabricServiceManager(@Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS) InetAddress hostname) {
    this.hostname = hostname;
    // this is to remember old log levels, will be used during reset
    this.oldLogLevels = Collections.synchronizedMap(new HashMap<String, LogEntry.Level>());
  }

  @Inject(optional = true)
  public void setLeaderElectionInfoService(LeaderElectionInfoService electionInfoService) {
    // Use optional Guice injection to make setup easier.
    // Currently all tools uses AppFabricServiceRuntimeModule().getDistributedModules() (which is bad),
    // which pull in this class.
    this.electionInfoService = electionInfoService;
  }

  @Override
  public String getDescription() {
    return Constants.AppFabric.SERVICE_DESCRIPTION;
  }

  @Override
  public int getMaxInstances() {
    return 1;
  }

  @Override
  public SystemServiceLiveInfo getLiveInfo() {
    SystemServiceLiveInfo.Builder builder = SystemServiceLiveInfo.builder();

    try {
      SortedMap<Integer, LeaderElectionInfoService.Participant> participants =
        electionInfoService.getParticipants(ELECTION_PARTICIPANTS_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      for (LeaderElectionInfoService.Participant participant : participants.values()) {
        builder.addContainer(new Containers.ContainerInfo(Containers.ContainerType.SYSTEM_SERVICE,
                                                          Constants.Service.APP_FABRIC_HTTP, null, null,
                                                          participant.getHostname(), null, null, null));
      }
    } catch (Exception e) {
      // If failed to get the leader election information, just return a static one with the current process
      builder.addContainer(new Containers.ContainerInfo(Containers.ContainerType.SYSTEM_SERVICE,
                                                        Constants.Service.APP_FABRIC_HTTP, null, null,
                                                        hostname.getHostName(), null, null, null));
    }

    return builder.build();
  }

  @Override
  public int getInstances() {
    return 1;
  }

  @Override
  public boolean setInstances(int instanceCount) {
    return false;
  }

  @Override
  public int getMinInstances() {
    return 1;
  }

  @Override
  public boolean isLogAvailable() {
    return true;
  }

  @Override
  public boolean canCheckStatus() {
    return true;
  }

  @Override
  public boolean isServiceAvailable() {
    return true;
  }

  @Override
  public boolean isServiceEnabled() {
    return true;
  }

  @Override
  public void restartAllInstances() {
    // no-op
  }

  @Override
  public void restartInstances(int instanceId, int... moreInstanceIds) {
    // no-op
  }

  @Override
  public void updateServiceLogLevels(Map<String, LogEntry.Level> logLevels) {
    LoggerContext context = getLoggerContext();
    if (context != null) {
      for (Map.Entry<String, LogEntry.Level> entry : logLevels.entrySet()) {
        String loggerName = entry.getKey();
        LogEntry.Level oldLogLevel = setLogLevel(context, loggerName, entry.getValue());
        // if logger name is not in oldLogLevels, we need to record its original log level
        if (!oldLogLevels.containsKey(loggerName)) {
          oldLogLevels.put(loggerName, oldLogLevel);
        }
      }
    }
  }

  @Override
  public void resetServiceLogLevels(Set<String> loggerNames) {
    LoggerContext context = getLoggerContext();
    Iterator<Map.Entry<String, LogEntry.Level>> entryIterator = oldLogLevels.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Map.Entry<String, LogEntry.Level> entry = entryIterator.next();
      String loggerName = entry.getKey();
      // logger name is empty if we are resetting all loggers.
      if (loggerNames.isEmpty() || loggerNames.contains(loggerName)) {
        setLogLevel(context, loggerName, entry.getValue());
        entryIterator.remove();
      }
    }
  }

  /**
   * Set the log level for the requested logger name.
   *
   * @param loggerName name of the logger
   * @param level the log level to set to.
   * @return the current log level of the given logger. If there is no log level configured for the given logger,
   *         {@code null} will be returned
   */
  @Nullable
  private LogEntry.Level setLogLevel(LoggerContext context, String loggerName, @Nullable LogEntry.Level level) {
    ch.qos.logback.classic.Logger logger = context.getLogger(loggerName);
    LogEntry.Level oldLogLevel = logger.getLevel() == null ? null :
      LogEntry.Level.valueOf(logger.getLevel().toString());
    LOG.debug("Log level of {} changed from {} to {}", loggerName, oldLogLevel, level);
    logger.setLevel(level == null ? null : Level.toLevel(level.name()));
    return oldLogLevel;
  }

  @Nullable
  private LoggerContext getLoggerContext() {
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (!(loggerFactory instanceof LoggerContext)) {
      LOG.warn("LoggerFactory is not a logback LoggerContext. No log appender is added. " +
                 "Logback might not be in the classpath");
      return null;
    }
    return (LoggerContext) loggerFactory;
  }
}
