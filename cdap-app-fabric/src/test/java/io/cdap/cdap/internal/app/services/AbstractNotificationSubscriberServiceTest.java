/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AbstractNotificationSubscriberService}.
 */
public class AbstractNotificationSubscriberServiceTest {

  private static final Gson GSON = new Gson();

  private static final class TestNotificationSubscriberService extends AbstractNotificationSubscriberService {

    private TestNotificationSubscriberService(
        String name,
        CConfiguration cConf,
        String topicName,
        MessagingService messagingService,
        TransactionRunner transactionRunner) {
      super(
          name,
          cConf,
          topicName,
          10,
          100L,
          messagingService,
          new NoOpMetricsCollectionService(),
          transactionRunner);
    }

    @Nullable
    @Override
    protected String loadMessageId(StructuredTableContext context) {
      return null;
    }

    @Override
    protected void storeMessageId(StructuredTableContext context, String messageId) {
    }

    @Override
    protected void processMessages(
        StructuredTableContext structuredTableContext,
        Iterator<ImmutablePair<String, Notification>> messages) {
    }
  }

  @Test
  public void testServiceNameContextAndDecodeMessage() {
    CConfiguration cConf = CConfiguration.create();
    MessagingService messagingService = Mockito.mock(MessagingService.class);
    TransactionRunner txRunner = Mockito.mock(TransactionRunner.class);

    TestNotificationSubscriberService service = new TestNotificationSubscriberService(
        "custom.notification.consumer", cConf, "testTopic", messagingService, txRunner);

    Assert.assertEquals("custom.notification.consumer", service.getServiceName());
    Assert.assertNotNull(service.getMessagingContext());
    Assert.assertSame(txRunner, service.getTransactionRunner());

    Notification expected = new Notification(
        Notification.Type.PROGRAM_STATUS, ImmutableMap.of("programRunId", "run-1"));
    String json = GSON.toJson(expected);
    Message message = new Message() {
      @Override
      public String getId() {
        return "msg-1";
      }

      @Override
      public byte[] getPayload() {
        return json.getBytes(StandardCharsets.UTF_8);
      }
    };

    Notification decoded = service.decodeMessage(message);
    Assert.assertEquals(expected, decoded);
  }
}
