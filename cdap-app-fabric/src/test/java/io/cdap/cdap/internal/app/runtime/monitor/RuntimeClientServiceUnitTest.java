/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.DatumWriterFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.messaging.spi.MessageFetchRequest;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Fast unit tests for {@link RuntimeClientService} self-stop on program termination.
 */
public class RuntimeClientServiceUnitTest {

  private static final Gson GSON = new Gson();

  @Test
  public void testSelfStopsWhenProgramTerminates() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("wf").run(RunIds.generate());
    CConfiguration cConf = CConfiguration.create();
    String statusTopic = Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC;
    cConf.set(Constants.RuntimeMonitor.TOPICS_CONFIGS, statusTopic);
    cConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 20L);
    cConf.setLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS, 40L);
    cConf.setInt(Constants.RuntimeMonitor.BATCH_SIZE, 10);

    Map<String, String> props = new HashMap<>();
    props.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId));
    props.put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.COMPLETED.name());
    props.put(ProgramOptionConstants.END_TIME, "1");
    Notification notification = new Notification(Notification.Type.PROGRAM_STATUS, props);

    byte[] rawMsgId = new byte[MessageId.RAW_ID_SIZE];
    MessageId.putRawId(1L, (short) 0, 1L, (short) 0, rawMsgId, 0);
    RawMessage rawMessage = new RawMessage.Builder()
        .setId(rawMsgId)
        .setPayload(GSON.toJson(notification).getBytes(StandardCharsets.UTF_8))
        .build();

    AtomicBoolean delivered = new AtomicBoolean(false);
    MessagingService messagingService = Mockito.mock(MessagingService.class);
    Mockito.when(messagingService.fetch(Mockito.any(MessageFetchRequest.class)))
        .thenAnswer(invocation -> {
          if (delivered.compareAndSet(false, true)) {
            return singleRawMessageIterator(rawMessage);
          }
          return CloseableIterator.empty();
        });

    RuntimeClient runtimeClient = Mockito.mock(RuntimeClient.class);
    Mockito.doAnswer(invocation -> {
      Iterator<Message> it = invocation.getArgument(2);
      while (it.hasNext()) {
        it.next();
      }
      return null;
    }).when(runtimeClient).sendMessages(Mockito.eq(programRunId), Mockito.any(TopicId.class), Mockito.any());

    RuntimeClientService service = new RuntimeClientService(
        cConf,
        messagingService,
        runtimeClient,
        programRunId,
        Mockito.mock(SchemaGenerator.class),
        Mockito.mock(DatumWriterFactory.class),
        Mockito.mock(DatumReaderFactory.class));

    service.startAndWait();
    Tasks.waitFor(Service.State.TERMINATED, service::state, 5, TimeUnit.SECONDS);
    Assert.assertNotNull(service.getProgramCompletionDetails());
    Assert.assertEquals(
        ProgramRunStatus.COMPLETED, service.getProgramCompletionDetails().getEndStatus());
    Assert.assertEquals(Bytes.toHexString(rawMsgId).length(), rawMsgId.length * 2);
  }

  private CloseableIterator<RawMessage> singleRawMessageIterator(RawMessage msg) {
    return new AbstractCloseableIterator<RawMessage>() {
      private boolean read;

      @Override
      protected RawMessage computeNext() {
        if (!read) {
          read = true;
          return msg;
        }
        return endOfData();
      }

      @Override
      public void close() {
        // no-op
      }
    };
  }
}
