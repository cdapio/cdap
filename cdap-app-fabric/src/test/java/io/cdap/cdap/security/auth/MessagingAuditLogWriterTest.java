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
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayDeque;
import java.util.Queue;

public class MessagingAuditLogWriterTest {
  @Test
  public void testPublish() throws Exception {
    MessagingService mockMsgService = Mockito.mock(MessagingService.class);

    Mockito.when(mockMsgService.publish(Mockito.any()))
        .thenThrow(new TopicNotFoundException("namespace", "topic"))
          .thenReturn(null);

    Mockito.doNothing().when(mockMsgService).createTopic(Mockito.any());

    MessagingAuditLogWriter messagingAuditLogWriter = new MessagingAuditLogWriter(CConfiguration.create(),
                                                                                  mockMsgService);
    Queue<AuditLogContext> auditLogContexts = new ArrayDeque<>();
    auditLogContexts.add(AuditLogContext.Builder.defaultNotRequired());
    auditLogContexts.add(new AuditLogContext.Builder()
                              .setAuditLoggingRequired(true)
                              .setAuditLogBody("Test Audit Logs")
                              .build());
    messagingAuditLogWriter.publish(auditLogContexts);

    //There should be at least 3 invocations. 1st will be a TopicNotFoundException , then 2 audit log events.
    Mockito.verify(mockMsgService, Mockito.atLeast(3)).publish(Mockito.any());
    //Single invocation to create a topic.
    Mockito.verify(mockMsgService, Mockito.times(1)).createTopic(Mockito.any());
  }
}
