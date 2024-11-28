/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.cdap.cdap.security.spi.authorization.AuditLogRequest;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class MessagingAuditLogWriterTest {
  @Test
  public void testPublish() throws Exception {
    MessagingService mockMsgService = Mockito.mock(MessagingService.class);

    Mockito.when(mockMsgService.publish(Mockito.any()))
        .thenThrow(new TopicNotFoundException("namespace", "topic"))
          .thenReturn(null);

    MessagingAuditLogWriter messagingAuditLogWriter = new MessagingAuditLogWriter(CConfiguration.create(),
                                                                                  mockMsgService);
    Queue<AuditLogContext> auditLogContexts = new ArrayDeque<>();
    auditLogContexts.add(AuditLogContext.Builder.defaultNotRequired());
    auditLogContexts.add(new AuditLogContext.Builder()
                              .setAuditLoggingRequired(true)
                              .setAuditLogBody("Test Audit Logs")
                              .build());
    AuditLogRequest auditLogRequest = new AuditLogRequest(
      200,
      "testUserIp",
      "v3/test",
      "Testhandler",
      "create",
      "POST",
      auditLogContexts,
      1000000L,
      1000002L);
    messagingAuditLogWriter.publish(auditLogRequest);

    Mockito.verify(mockMsgService, Mockito.atLeast(1)).publish(Mockito.any());
  }
}
