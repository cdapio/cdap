/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
import com.google.common.collect.ImmutableMap;
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
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;

import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * An implementation of ProgramStateWriter that publishes program status events to TMS
 */
public final class MessagingProgramStateWriter implements ProgramStateWriter {

  private static final Gson GSON =
    ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec()).create();

  private final ProgramStatePublisher programStatePublisher;


  @Inject
  public MessagingProgramStateWriter(CConfiguration cConf, MessagingService messagingService) {
    this(new MessagingProgramStatePublisher(messagingService,
                                            NamespaceId.SYSTEM.topic(cConf.get(
                                              Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)),
                                            RetryStrategies.fromConfiguration(cConf, "system.program.state.")));
  }

  @VisibleForTesting
  public MessagingProgramStateWriter(ProgramStatePublisher programStatePublisher) {
    this.programStatePublisher = programStatePublisher;
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
  public void running(ProgramRunId programRunId, @Nullable String twillRunId) {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
      .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
      .put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(System.currentTimeMillis()))
      .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RUNNING.name());
    if (twillRunId != null) {
      properties.put(ProgramOptionConstants.TWILL_RUN_ID, twillRunId);
    }
    programStatePublisher.publish(Notification.Type.PROGRAM_STATUS, properties.build());
  }

  @Override
  public void completed(ProgramRunId programRunId) {
    stop(programRunId, ProgramRunStatus.COMPLETED, null);
  }

  @Override
  public void killed(ProgramRunId programRunId) {
    stop(programRunId, ProgramRunStatus.KILLED, null);
  }

  @Override
  public void error(ProgramRunId programRunId, Throwable failureCause) {
    stop(programRunId, ProgramRunStatus.FAILED, failureCause);
  }

  @Override
  public void suspend(ProgramRunId programRunId) {
    programStatePublisher.publish(Notification.Type.PROGRAM_STATUS,
                                  ImmutableMap.<String, String>builder()
                                    .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
                                    .put(ProgramOptionConstants.SUSPEND_TIME,
                                         String.valueOf(System.currentTimeMillis()))
                                    .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.SUSPENDED.name())
                                    .build()
    );
  }

  @Override
  public void resume(ProgramRunId programRunId) {
    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
      .put(ProgramOptionConstants.RESUME_TIME, String.valueOf(System.currentTimeMillis()))
      .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RESUMING.name()).build();
    programStatePublisher.publish(Notification.Type.PROGRAM_STATUS, properties);
  }

  @Override
  public void reject(ProgramRunId programRunId, ProgramOptions programOptions,
                     ProgramDescriptor programDescriptor, String userId, Throwable cause) {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
      .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
      .put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(programOptions.getUserArguments().asMap()))
      .put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(programOptions.getArguments().asMap()))
      .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.REJECTED.name())
      .put(ProgramOptionConstants.USER_ID, userId)
      .put(ProgramOptionConstants.PROGRAM_DESCRIPTOR, GSON.toJson(programDescriptor))
      .put(ProgramOptionConstants.PROGRAM_ERROR, GSON.toJson(new BasicThrowable(cause)));
    programStatePublisher.publish(Notification.Type.PROGRAM_STATUS, properties.build());
  }

  private void stop(ProgramRunId programRunId, ProgramRunStatus runStatus, @Nullable Throwable cause) {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
      .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
      .put(ProgramOptionConstants.END_TIME, String.valueOf(System.currentTimeMillis()))
      .put(ProgramOptionConstants.PROGRAM_STATUS, runStatus.name());
    if (cause != null) {
      properties.put(ProgramOptionConstants.PROGRAM_ERROR, GSON.toJson(new BasicThrowable(cause)));
    }
    programStatePublisher.publish(Notification.Type.PROGRAM_STATUS, properties.build());
  }

}
