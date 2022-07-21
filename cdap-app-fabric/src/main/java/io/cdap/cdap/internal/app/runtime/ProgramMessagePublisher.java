/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime;

import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.messaging.context.AbstractMessagePublisher;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;

import java.io.IOException;
import java.util.Iterator;

/**
 * A {@link MessagePublisher} for program to use, which doesn't allow publishing to system namespace.
 */
final class ProgramMessagePublisher extends AbstractMessagePublisher {

  private final MessagePublisher delegate;

  ProgramMessagePublisher(MessagePublisher delegate) {
    this.delegate = delegate;
  }

  @Override
  protected void publish(TopicId topicId, Iterator<byte[]> payloads)
    throws IOException, TopicNotFoundException, AccessException {
    if (NamespaceId.SYSTEM.equals(topicId.getNamespaceId())) {
      throw new IllegalArgumentException("Publish to '" + topicId.getNamespace() + "' namespace is not allowed");
    }
    delegate.publish(topicId.getNamespace(), topicId.getTopic(), payloads);
  }
}
