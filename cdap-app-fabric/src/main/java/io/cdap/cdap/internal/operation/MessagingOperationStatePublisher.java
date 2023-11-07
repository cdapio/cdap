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

package io.cdap.cdap.internal.operation;

import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationResource;
import java.util.Set;

/**
 * Provides capabilities to send operation lifecycle specific messages.
 */
public class MessagingOperationStatePublisher implements OperationStatePublisher {

  private final MessagingService messagingService;

  public MessagingOperationStatePublisher(MessagingService messagingService) {
    this.messagingService = messagingService;
  }

  @Override
  public void publishResources(OperationRunId runId, Set<OperationResource> resources) {
    // TODO(samik) implement message publish logic
  }

  @Override
  public void publishRunning(OperationRunId runId) {
    // TODO(samik) implement message publish logic
  }

  @Override
  public void publishFailed(OperationRunId runId, OperationError error) {
    // TODO(samik) implement message publish logic

  }

  @Override
  public void publishSuccess(OperationRunId runId) {
    // TODO(samik) implement message publish logic

  }

  @Override
  public void publishStopped(OperationRunId runId) {
    // TODO(samik) implement message publish logic
  }
}
