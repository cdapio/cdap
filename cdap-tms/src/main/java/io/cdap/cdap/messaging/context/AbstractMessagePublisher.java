/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.messaging.context;

import com.google.common.collect.Iterators;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.io.ByteBuffers;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * An abstract implementation of {@link MessagePublisher} that implement most of the publish methods by
 * delegating to the {@link #publish(String, String, Iterator)}.
 */
public abstract class AbstractMessagePublisher implements MessagePublisher {

  @Override
  public final void publish(String namespace,
                            String topic, String... payloads)
    throws IOException, TopicNotFoundException, AccessException {
    publish(namespace, topic, StandardCharsets.UTF_8, Iterators.forArray(payloads));
  }

  @Override
  public final void publish(String namespace, String topic,
                            Charset charset, String... payloads)
    throws IOException, TopicNotFoundException, AccessException {
    publish(namespace, topic, charset, Iterators.forArray(payloads));
  }

  @Override
  public final void publish(String namespace,
                            String topic, byte[]... payloads)
    throws IOException, TopicNotFoundException, AccessException {
    publish(namespace, topic, Iterators.forArray(payloads));
  }

  @Override
  public final void publish(String namespace, String topic, final Charset charset,
                            Iterator<String> payloads)
    throws IOException, TopicNotFoundException, AccessException {
    publish(namespace, topic, Iterators.transform(payloads, input -> ByteBuffers.getByteArray(charset.encode(input))));
  }

  @Override
  public final void publish(String namespace, String topic,
                            Iterator<byte[]> payloads)
    throws TopicNotFoundException, IOException, AccessException {
    NamespaceId namespaceId = new NamespaceId(namespace);
    publish(namespaceId.topic(topic), payloads);
  }

  /**
   * Publishes payloads to the given topic.
   */
  protected abstract void publish(TopicId topicId,
                                  Iterator<byte[]> payloads)
    throws IOException, TopicNotFoundException, AccessException;
}
