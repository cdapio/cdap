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

package co.cask.cdap.internal.app.program;

import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * An implementation of ProgramStateWriter that publishes program status events to TMS
 */
public final class MessagingProgramStateWriter implements ProgramStateWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MessagingProgramStateWriter.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  // TODO move constant to right location after making it configurable

  private final ProgramStatePublisher programStatePublisher;

  private ScheduledExecutorService scheduledExecutorService;

  private final long heartBeatIntervalSeconds;

  @Inject
  public MessagingProgramStateWriter(CConfiguration cConf, MessagingService messagingService) {
    this(new MessagingProgramStatePublisher(messagingService,
                                            NamespaceId.SYSTEM.topic(cConf.get(
                                              Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)),
                                            RetryStrategies.fromConfiguration(cConf, "system.program.state.")),
         cConf.getLong(Constants.ProgramHeartbeat.HEARTBEAT_INTERVAL_SECONDS,
                       Constants.ProgramHeartbeat.DEFAULT_HEARTBEAT_INTERVAL_SECONDS));
  }

  @VisibleForTesting
  MessagingProgramStateWriter(ProgramStatePublisher programStatePublisher, long heartBeatIntervalSeconds) {
    this.programStatePublisher = programStatePublisher;
    this.heartBeatIntervalSeconds = heartBeatIntervalSeconds;
  }

  @Override
  public void start(ProgramRunId programRunId, ProgramOptions programOptions, @Nullable String twillRunId,
                    ProgramDescriptor programDescriptor) {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
      .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
      .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.STARTING.name())
      .put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(programOptions.getUserArguments().asMap()))
      .put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(programOptions.getArguments().asMap()))
      .put(ProgramOptionConstants.PROGRAM_DESCRIPTOR, GSON.toJson(programDescriptor));

    if (twillRunId != null) {
      properties.put(ProgramOptionConstants.TWILL_RUN_ID, twillRunId);
    }
    programStatePublisher.publish(Notification.Type.PROGRAM_STATUS, properties.build());
  }

  @Override
  public void running(ProgramRunId programRunId, @Nullable String twillRunId, ProgramOptions programOptions) {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
      .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
      .put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(System.currentTimeMillis()))
      .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RUNNING.name())
      .put(ProgramOptionConstants.PROGRAM_OPTIONS, GSON.toJson(programOptions));

    if (twillRunId != null) {
      properties.put(ProgramOptionConstants.TWILL_RUN_ID, twillRunId);
    }
    programStatePublisher.publish(Notification.Type.PROGRAM_STATUS, properties.build());
    scheduleHeartBeatThread(properties.build());
  }

  /**
   * if executor service isn't initialized or if its shutdown
   * create a new exector service and schedule a heartbeat thread
   * @param properties
   */
  private void scheduleHeartBeatThread(Map<String, String> properties) {
    if (scheduledExecutorService == null || scheduledExecutorService.isShutdown()) {
      scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("program-heart-beat"));
      ProgramRunHeartbeat programRunHeartbeat = new ProgramRunHeartbeat(programStatePublisher, properties);
      scheduledExecutorService.scheduleAtFixedRate(programRunHeartbeat, heartBeatIntervalSeconds,
                                                   heartBeatIntervalSeconds, TimeUnit.SECONDS);
    }
  }

  @Override
  public void completed(ProgramRunId programRunId, ProgramOptions programOptions) {
    stop(programRunId, ProgramRunStatus.COMPLETED, null, programOptions);
  }

  @Override
  public void killed(ProgramRunId programRunId, ProgramOptions programOptions) {
    stop(programRunId, ProgramRunStatus.KILLED, null, programOptions);
  }

  @Override
  public void error(ProgramRunId programRunId, Throwable failureCause, ProgramOptions programOptions) {
    stop(programRunId, ProgramRunStatus.FAILED, failureCause, programOptions);
  }

  @Override
  public void suspend(ProgramRunId programRunId, ProgramOptions programOptions) {
    programStatePublisher.publish(Notification.Type.PROGRAM_STATUS,
                                  ImmutableMap.<String, String>builder()
                                    .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
                                    .put(ProgramOptionConstants.SUSPEND_TIME,
                                         String.valueOf(System.currentTimeMillis()))
                                    .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.SUSPENDED.name())
                                    .put(ProgramOptionConstants.PROGRAM_OPTIONS, GSON.toJson(programOptions)).build()
    );
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdownNow();
    }
  }

  @Override
  public void resume(ProgramRunId programRunId, ProgramOptions programOptions) {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
      .put(ProgramOptionConstants.RESUME_TIME, String.valueOf(System.currentTimeMillis()))
      .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RESUMING.name())
      .put(ProgramOptionConstants.PROGRAM_OPTIONS, GSON.toJson(programOptions)).build();
    programStatePublisher.publish(Notification.Type.PROGRAM_STATUS, properties);
    scheduleHeartBeatThread(properties);
  }

  private void stop(ProgramRunId programRunId, ProgramRunStatus runStatus, @Nullable Throwable cause,
                    ProgramOptions options) {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
      .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
      .put(ProgramOptionConstants.END_TIME, String.valueOf(System.currentTimeMillis()))
      .put(ProgramOptionConstants.PROGRAM_STATUS, runStatus.name())
      .put(ProgramOptionConstants.PROGRAM_OPTIONS, GSON.toJson(options));
    if (cause != null) {
      properties.put(ProgramOptionConstants.PROGRAM_ERROR, GSON.toJson(new BasicThrowable(cause)));
    }
    programStatePublisher.publish(Notification.Type.PROGRAM_STATUS, properties.build());
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdownNow();
    }
  }

  @VisibleForTesting
  boolean isHeartBeatThreadAlive() {
    return scheduledExecutorService != null && !scheduledExecutorService.isShutdown();
  }
}
