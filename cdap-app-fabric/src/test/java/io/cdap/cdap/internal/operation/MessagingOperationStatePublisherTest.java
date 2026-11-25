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

package io.cdap.cdap.internal.operation;

import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.conf.Constants.Operation;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationResource;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link MessagingOperationStatePublisher}.
 */
public class MessagingOperationStatePublisherTest {

  private static final Gson GSON = new Gson();

  @Test
  public void testPublishStatesAndTopicPartitioning() throws Exception {
    MessagingService messagingService = Mockito.mock(MessagingService.class);
    MessagingOperationStatePublisher singleTopicPublisher = new MessagingOperationStatePublisher(
        messagingService, "op_status_", 1, RetryStrategies.noRetry());
    OperationRunId runId = new OperationRunId("ns1", "run-123");

    singleTopicPublisher.publishStarting(runId);
    singleTopicPublisher.publishRunning(runId);
    singleTopicPublisher.publishResources(runId, ImmutableSet.of(new OperationResource("res1")));
    singleTopicPublisher.publishStopping(runId);
    singleTopicPublisher.publishSuccess(runId);
    singleTopicPublisher.publishFailed(
        runId, new OperationError("err", Collections.emptyList()));
    singleTopicPublisher.publishKilled(runId);

    ArgumentCaptor<StoreRequest> requestCaptor = ArgumentCaptor.forClass(StoreRequest.class);
    Mockito.verify(messagingService, Mockito.times(7)).publish(requestCaptor.capture());
    for (StoreRequest request : requestCaptor.getAllValues()) {
      Assert.assertEquals(NamespaceId.SYSTEM.topic("op_status_0"), request.getTopicId());
    }
    String firstPayload =
        new String(requestCaptor.getAllValues().get(0).iterator().next(), StandardCharsets.UTF_8);
    Notification firstNotification = GSON.fromJson(firstPayload, Notification.class);
    Assert.assertEquals(Notification.Type.OPERATION_STATUS, firstNotification.getNotificationType());
    Assert.assertEquals(
        OperationRunStatus.STARTING.name(),
        firstNotification.getProperties().get(Operation.STATUS_NOTIFICATION_KEY));

    MessagingService partitionedMessaging = Mockito.mock(MessagingService.class);
    MessagingOperationStatePublisher multiTopicPublisher = new MessagingOperationStatePublisher(
        partitionedMessaging, "op_part_", 3, RetryStrategies.noRetry());
    multiTopicPublisher.publishRunning(runId);
    ArgumentCaptor<StoreRequest> multiCaptor = ArgumentCaptor.forClass(StoreRequest.class);
    Mockito.verify(partitionedMessaging).publish(multiCaptor.capture());
    int expectedIdx = Math.abs(runId.getRun().hashCode()) % 3;
    Assert.assertEquals(
        NamespaceId.SYSTEM.topic("op_part_" + expectedIdx),
        multiCaptor.getValue().getTopicId());
  }

  @Test
  public void testRetryTransientExceptionsAndExhaustedThrows() throws Exception {
    MessagingService messagingService = Mockito.mock(MessagingService.class);
    AtomicInteger attempts = new AtomicInteger();
    Mockito.doAnswer(invocation -> {
      int n = attempts.incrementAndGet();
      if (n == 1) {
        throw new TopicNotFoundException("system", "op_0");
      }
      if (n == 2) {
        throw new ServiceUnavailableException("messaging");
      }
      return null;
    }).when(messagingService).publish(Mockito.any(StoreRequest.class));

    RetryStrategy fastRetry = RetryStrategies.limit(3, RetryStrategies.fixDelay(1, TimeUnit.MILLISECONDS));
    MessagingOperationStatePublisher publisher =
        new MessagingOperationStatePublisher(messagingService, "op_", 1, fastRetry);
    publisher.publishRunning(new OperationRunId("ns1", "run1"));
    Assert.assertEquals(3, attempts.get());

    MessagingService failingService = Mockito.mock(MessagingService.class);
    TopicNotFoundException topicEx = new TopicNotFoundException("system", "op_0");
    Mockito.doThrow(topicEx).when(failingService).publish(Mockito.any(StoreRequest.class));
    MessagingOperationStatePublisher noRetryPublisher = new MessagingOperationStatePublisher(
        failingService, "op_", 1, RetryStrategies.noRetry());
    try {
      noRetryPublisher.publishRunning(new OperationRunId("ns1", "run2"));
      Assert.fail("Expected RuntimeException wrapping TopicNotFoundException");
    } catch (RuntimeException e) {
      Assert.assertSame(topicEx, e.getCause());
    }
  }

  @Test
  public void testPropagatesCheckedWrappedAndUncheckedUnwrapped() throws Exception {
    MessagingService ioFailService = Mockito.mock(MessagingService.class);
    IOException ioEx = new IOException("disk full");
    Mockito.doThrow(ioEx).when(ioFailService).publish(Mockito.any(StoreRequest.class));
    MessagingOperationStatePublisher ioPublisher = new MessagingOperationStatePublisher(
        ioFailService, "op_", 1, RetryStrategies.noRetry());
    try {
      ioPublisher.publishRunning(new OperationRunId("ns1", "run-io"));
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException e) {
      Assert.assertSame(ioEx, e.getCause());
    }

    MessagingService accessFailService = Mockito.mock(MessagingService.class);
    AccessException accessEx = new AccessException("denied");
    Mockito.doThrow(accessEx).when(accessFailService).publish(Mockito.any(StoreRequest.class));
    MessagingOperationStatePublisher accessPublisher = new MessagingOperationStatePublisher(
        accessFailService, "op_", 1, RetryStrategies.noRetry());
    try {
      accessPublisher.publishRunning(new OperationRunId("ns1", "run-access"));
      Assert.fail("Expected AccessException propagated unwrapped");
    } catch (AccessException e) {
      Assert.assertSame(accessEx, e);
    }

    MessagingService runtimeFailService = Mockito.mock(MessagingService.class);
    IllegalArgumentException badArg = new IllegalArgumentException("bad payload");
    Mockito.doThrow(badArg).when(runtimeFailService).publish(Mockito.any(StoreRequest.class));
    MessagingOperationStatePublisher runtimePublisher = new MessagingOperationStatePublisher(
        runtimeFailService, "op_", 1, RetryStrategies.noRetry());
    try {
      runtimePublisher.publishRunning(new OperationRunId("ns1", "run-rt"));
      Assert.fail("Expected IllegalArgumentException unwrapped");
    } catch (IllegalArgumentException e) {
      Assert.assertSame(badArg, e);
    }
  }
}
