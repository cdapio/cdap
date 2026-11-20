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

package io.cdap.cdap.internal.profile;

import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ScheduleId;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AdminEventPublisher}.
 */
public class AdminEventPublisherTest {

  @Test
  public void testPublishRetriesTransientAndSucceeds() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    MessagingContext messagingContext = Mockito.mock(MessagingContext.class);
    MessagePublisher messagePublisher = Mockito.mock(MessagePublisher.class);
    Mockito.when(messagingContext.getMessagePublisher()).thenReturn(messagePublisher);

    AtomicInteger attempts = new AtomicInteger();
    Mockito.doAnswer(invocation -> {
      int n = attempts.incrementAndGet();
      if (n == 1) {
        throw new TopicNotFoundException("system", "metadata");
      }
      if (n == 2) {
        throw new ServiceUnavailableException("tms");
      }
      return null;
    }).when(messagePublisher).publish(
        Mockito.anyString(), Mockito.anyString(), Mockito.<String[]>any());

    AdminEventPublisher publisher = new AdminEventPublisher(cConf, messagingContext);
    NamespaceId targetNamespace = new NamespaceId("ns1");
    publisher.publishProfileAssignment(targetNamespace, 42L);

    Assert.assertEquals(3, attempts.get());
    ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(messagePublisher, Mockito.times(3)).publish(
        Mockito.eq(NamespaceId.SYSTEM.getNamespace()),
        Mockito.eq(cConf.get(Constants.Metadata.MESSAGING_TOPIC)),
        payloadCaptor.capture());
    Assert.assertTrue(payloadCaptor.getValue().contains("PROFILE_ASSIGNMENT"));

    ScheduleId scheduleId = targetNamespace.app("app1").schedule("sched1");
    publisher.publishScheduleCreation(scheduleId, 100L);
    publisher.publishProfileUnAssignment(targetNamespace, 43L);
    Mockito.verify(messagePublisher, Mockito.times(5)).publish(
        Mockito.eq(NamespaceId.SYSTEM.getNamespace()),
        Mockito.eq(cConf.get(Constants.Metadata.MESSAGING_TOPIC)),
        Mockito.<String[]>any());
  }

  @Test
  public void testPublishNonRetryableExceptionsFailFastWithCause() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    MessagingContext messagingContext = Mockito.mock(MessagingContext.class);
    MessagePublisher messagePublisher = Mockito.mock(MessagePublisher.class);
    Mockito.when(messagingContext.getMessagePublisher()).thenReturn(messagePublisher);

    IOException ioEx = new IOException("broken pipe");
    Mockito.doThrow(ioEx).when(messagePublisher).publish(
        Mockito.anyString(), Mockito.anyString(), Mockito.<String[]>any());

    AdminEventPublisher publisher = new AdminEventPublisher(cConf, messagingContext);
    NamespaceId targetNamespace = new NamespaceId("ns1");
    try {
      publisher.publishProfileAssignment(targetNamespace, 1L);
      Assert.fail("Expected RuntimeException on IOException");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains(targetNamespace.toString()));
      Assert.assertSame(ioEx, e.getCause().getCause());
    }

    Mockito.reset(messagePublisher);
    AccessException accessEx = new AccessException("unauthorized");
    Mockito.doThrow(accessEx).when(messagePublisher).publish(
        Mockito.anyString(), Mockito.anyString(), Mockito.<String[]>any());
    try {
      publisher.publishProfileUnAssignment(targetNamespace, 2L);
      Assert.fail("Expected RuntimeException on AccessException");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains(targetNamespace.toString()));
      Assert.assertSame(accessEx, e.getCause());
    }
  }
}
