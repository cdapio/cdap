/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.logging.gateway.handlers.ProgramRunRecordFetcher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class TetheringProgramEventPublisherTest {
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
      new GsonBuilder())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .create();

  @Rule
  public MockitoRule mockitorule = MockitoJUnit.rule();

  @Mock
  private TransactionRunner transactionRunner;

  @Mock
  private TetheringStore store;

  @Mock
  private MessagingService messagingService;

  @Mock
  private ProgramRunRecordFetcher recordFetcher;

  private TetheringProgramEventPublisher publisher;

  private ProgramRunId programRunId;

  private TestMessageFetcher messageFetcher;

  @Before
  public void setup() {
    messageFetcher = new TestMessageFetcher();
    publisher = new TetheringProgramEventPublisher(CConfiguration.create(),
                                                   store, messagingService, messageFetcher,
                                                   recordFetcher, transactionRunner);
    programRunId = new ProgramRunId("namespace", "application",
                                    ProgramType.SPARK, "program", "testRun");
  }

  @Test
  public void testIgnoreNonProgramStateUpdates() {
    Notification notification = new Notification(Notification.Type.PROGRAM_HEART_BEAT, Collections.emptyMap());
    String messageId = "1001";
    Message message = new Message() {
      @Override
      public String getId() {
        return messageId;
      }

      @Override
      public byte[] getPayload() {
        return GSON.toJson(notification).getBytes(StandardCharsets.UTF_8);
      }
    };
    messageFetcher.setMessages(Collections.singletonList(message).iterator());

    TetheringProgramEventPublisher.PeerProgramUpdates peerProgramUpdates = publisher.getPeerProgramUpdates();

    // We should ignore non program state update messages, but last message id should be updated
    Assert.assertEquals(messageId, peerProgramUpdates.lastMessageId);
    Assert.assertEquals(Collections.emptyMap(), peerProgramUpdates.peerToNotifications);
  }

  @Test
  public void testIgnoreNonTetheredProgramStateUpdates() throws NotFoundException, IOException {
    Map<String, String> properties = ImmutableMap.of(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId));
    Notification notification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    String messageId = "1002";
    Message message = new Message() {
      @Override
      public String getId() {
        return messageId;
      }

      @Override
      public byte[] getPayload() {
        return GSON.toJson(notification).getBytes(StandardCharsets.UTF_8);
      }
    };
    messageFetcher.setMessages(Collections.singletonList(message).iterator());

    RunRecordDetail runRecord = RunRecordDetail.builder()
      .setProgramRunId(programRunId)
      .setStartTime(System.currentTimeMillis())
      .setSourceId(new byte[MessageId.RAW_ID_SIZE])
      .build();
    // Peer name is not set in the run record
    Mockito.when(recordFetcher.getRunRecordMeta(programRunId))
      .thenReturn(runRecord);

    TetheringProgramEventPublisher.PeerProgramUpdates peerProgramUpdates = publisher.getPeerProgramUpdates();

    // We should ignore program updates for non-tethered runs, but last message id should be updated
    Assert.assertEquals(messageId, peerProgramUpdates.lastMessageId);
    Assert.assertEquals(Collections.emptyMap(), peerProgramUpdates.peerToNotifications);
  }

  static class TestMessageFetcher implements MessageFetcher {
    private Iterator<Message> messages;

    public void setMessages(Iterator<Message> messages) {
      this.messages = messages;
    }

    @Override
    public CloseableIterator<Message> fetch(String namespace, String topic, int limit, long timestamp) {
      return null;
    }

    @Override
    public CloseableIterator<Message> fetch(String namespace, String topic, int limit,
                                            @Nullable String afterMessageId) {
      return new AbstractCloseableIterator<Message>() {
        @Override
        protected Message computeNext() {
          return messages.hasNext() ? messages.next() : endOfData();
        }

        @Override
        public void close() {}
      };
    }
  }
}
