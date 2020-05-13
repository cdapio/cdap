/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Injector;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.program.MessagingProgramStatePublisher;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.program.ProgramStatePublisher;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeClient;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.provision.NativeProvisioner;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link RuntimeServiceMain}.
 */
public class RuntimeServiceMainTest extends MasterServiceMainTestBase {

  private static final Gson GSON = new Gson();

  @Test
  public void testRuntimeService() throws Exception {
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("test", "1.0");
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").worker("worker").run(RunIds.generate());

    Map<String, String> systemArgs = ImmutableMap.of(
      SystemArguments.PROFILE_PROVISIONER, NativeProvisioner.SPEC.getName(),
      SystemArguments.PROFILE_NAME, "default"
    );

    ProgramOptions programOptions = new SimpleProgramOptions(programRunId.getParent(), new BasicArguments(systemArgs),
                                                             new BasicArguments());
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programRunId.getParent(), null, artifactId);

    // Write out program state events to simulate program start
    Injector appFabricInjector = getServiceMainInstance(AppFabricServiceMain.class).getInjector();
    CConfiguration cConf = appFabricInjector.getInstance(CConfiguration.class);
    ProgramStatePublisher programStatePublisher = new MessagingProgramStatePublisher(
      appFabricInjector.getInstance(MessagingService.class),
      NamespaceId.SYSTEM.topic(cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC)),
      RetryStrategies.fromConfiguration(cConf, "system.program.state.")
    );
    new MessagingProgramStateWriter(programStatePublisher).start(programRunId, programOptions, null,
                                                                 programDescriptor);

    Injector injector = getServiceMainInstance(RuntimeServiceMain.class).getInjector();
    TransactionRunner txRunner = injector.getInstance(TransactionRunner.class);

    // Should see a STARTING record in the runtime store
    Tasks.waitFor(ProgramRunStatus.STARTING, () -> {
      RunRecordDetail detail = TransactionRunners.run(txRunner, context -> {
        return AppMetadataStore.create(context).getRun(programRunId);
      });
      return detail == null ? null : detail.getStatus();
    }, 5, TimeUnit.SECONDS);

    ProgramStateWriter programStateWriter = createProgramStateWriter(injector, programRunId);


    // Write a running state. We should see a RUNNING record in the runtime store
    programStateWriter.running(programRunId, null);
    Tasks.waitFor(ProgramRunStatus.RUNNING, () -> {
      RunRecordDetail detail = TransactionRunners.run(txRunner, context -> {
        return AppMetadataStore.create(context).getRun(programRunId);
      });
      return detail == null ? null : detail.getStatus();
    }, 5, TimeUnit.SECONDS);

    // Write a complete state. The run record should be removed in the runtime store
    programStateWriter.completed(programRunId);
    Tasks.waitFor(true, () -> TransactionRunners.run(
      txRunner, context -> AppMetadataStore.create(context).getRun(programRunId) == null), 5, TimeUnit.SECONDS);
  }

  /**
   * Creates a {@link ProgramStateWriter} that writes to {@link RuntimeClient} directly.
   *
   * @param injector the injector for creating the {@link RuntimeClient}
   * @param programRunId the {@link ProgramRunId} for the program state change
   * @return a {@link ProgramStateWriter}
   */
  private ProgramStateWriter createProgramStateWriter(Injector injector, ProgramRunId programRunId) {
    RuntimeClient runtimeClient = injector.getInstance(RuntimeClient.class);

    // We write to the record event directly to skip the app-fabric to process it
    // This is because we don't follow the normal event flow here for testing
    TopicId topicId = NamespaceId.SYSTEM.topic(
      injector.getInstance(CConfiguration.class).get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC));
    RetryStrategy retryStrategy = RetryStrategies.timeLimit(5, TimeUnit.SECONDS,
                                                            RetryStrategies.fixDelay(200, TimeUnit.MILLISECONDS));

    return new MessagingProgramStateWriter((notificationType, properties) -> {
      Notification notification = new Notification(notificationType, properties);
      try {
        Retries.callWithRetries((Retries.Callable<Void, Exception>) () -> {
          runtimeClient.sendMessages(programRunId, topicId,
                                     Collections.singleton(createMessage(notification)).iterator());
          return null;
        }, retryStrategy, t -> t instanceof IOException || t instanceof RetryableException);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private Message createMessage(Notification notification) {
    byte[] payload = GSON.toJson(notification).getBytes(StandardCharsets.UTF_8);
    return new Message() {
      @Override
      public String getId() {
        byte[] id = new byte[MessageId.RAW_ID_SIZE];
        MessageId.putRawId(System.currentTimeMillis(), (short) 0, 0, (short) 0, id, 0);
        return Bytes.toHexString(id);
      }

      @Override
      public byte[] getPayload() {
        return payload;
      }
    };
  }
}
