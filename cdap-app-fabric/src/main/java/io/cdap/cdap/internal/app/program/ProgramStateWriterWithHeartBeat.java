/*
 * Copyright © 2018 Cask Data, Inc.
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
package io.cdap.cdap.internal.app.program;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Wrapper around {@link ProgramStateWriter} with additional hook to start a
 * heartbeat thread on running or resuming program state and stop the thread on completed/error/suspend state.
 */
public class ProgramStateWriterWithHeartBeat {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramStateWriterWithHeartBeat.class);
  private static final Gson GSON =
    ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec()).create();
  private final long heartBeatIntervalSeconds;
  private final ProgramStateWriter programStateWriter;
  private final ProgramRunId programRunId;
  private final ProgramStatePublisher messagingProgramStatePublisher;
  private ScheduledExecutorService scheduledExecutorService;


  public ProgramStateWriterWithHeartBeat(ProgramRunId programRunId,
                                         ProgramStateWriter programStateWriter,
                                         MessagingService messagingService,
                                         CConfiguration cConf) {
    this(programRunId, programStateWriter, cConf.getLong(Constants.ProgramHeartbeat.HEARTBEAT_INTERVAL_SECONDS),
         new MessagingProgramStatePublisher(messagingService,
                                            NamespaceId.SYSTEM.topic(cConf.get(
                                              Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)),
                                            RetryStrategies.fromConfiguration(cConf, "system.program.state.")));
  }

  @VisibleForTesting
  ProgramStateWriterWithHeartBeat(ProgramRunId programRunId,
                                  ProgramStateWriter programStateWriter,
                                  long heartBeatIntervalSeconds,
                                  ProgramStatePublisher messagingProgramStatePublisher) {
    this.programRunId = programRunId;
    this.programStateWriter = programStateWriter;
    this.heartBeatIntervalSeconds = heartBeatIntervalSeconds;
    this.messagingProgramStatePublisher = messagingProgramStatePublisher;
  }

  public void start(ProgramOptions programOptions, @Nullable String twillRunId, ProgramDescriptor programDescriptor) {
    programStateWriter.start(programRunId, programOptions, twillRunId, programDescriptor);
  }

  public void running(@Nullable String twillRunId) {
    programStateWriter.running(programRunId, twillRunId);
    scheduleHeartBeatThread();
  }

  public void completed() {
    stopHeartbeatThread();
    programStateWriter.completed(programRunId);
  }

  public void killed() {
    stopHeartbeatThread();
    programStateWriter.killed(programRunId);
  }

  public void error(Throwable failureCause) {
    stopHeartbeatThread();
    programStateWriter.error(programRunId, failureCause);
  }

  public void suspend() {
    stopHeartbeatThread();
    programStateWriter.suspend(programRunId);
  }

  public void resume() {
    scheduleHeartBeatThread();
    programStateWriter.resume(programRunId);
  }

  public void initiateGracefulStop() {
    long timeout = -1; // TODO where do I get this value from?
    scheduleHeartBeatThread();
    programStateWriter.stopping(programRunId, timeout);
  }

  /**
   * If executor service isn't initialized or if its shutdown
   * create a new executor service and schedule a heartbeat thread
   */
  private void scheduleHeartBeatThread() {
    if (scheduledExecutorService == null || scheduledExecutorService.isShutdown()) {
      scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("program-heart-beat"));
      scheduledExecutorService.scheduleAtFixedRate(
        () -> {
          Map<String, String> properties = new HashMap<>();
          properties.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId));
          properties.put(ProgramOptionConstants.HEART_BEAT_TIME, String.valueOf(System.currentTimeMillis()));
          // publish as heart_beat type, so it can be handled appropriately at receiver
          messagingProgramStatePublisher.publish(Notification.Type.PROGRAM_HEART_BEAT, properties);
          LOG.trace("Sent heartbeat for program {}", programRunId);
        }, heartBeatIntervalSeconds,
        heartBeatIntervalSeconds, TimeUnit.SECONDS);
    }
  }

  private void stopHeartbeatThread() {
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdownNow();
    }
  }

  /**
   * This method is only used for testing
   * @return true if the heart beat thread is active, false otherwise
   */
  @VisibleForTesting
  boolean isHeartBeatThreadAlive() {
    return scheduledExecutorService != null && !scheduledExecutorService.isShutdown();
  }
}
