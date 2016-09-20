/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.queue;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.app.queue.InputDatum;
import co.cask.cdap.app.queue.QueueReader;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.stream.StreamConsumer;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A {@link QueueReader} implementation that dequeue from stream
 *
 * @param <T> Target type.
 */
public final class StreamQueueReader<T> implements QueueReader<T> {

  private final StreamId streamId;
  private final Supplier<StreamConsumer> consumerSupplier;
  private final int batchSize;
  private final Function<StreamEvent, T> eventTransform;
  private final Principal principal;
  private final AuthorizationEnforcer authorizationEnforcer;


  StreamQueueReader(StreamId streamId, Supplier<StreamConsumer> consumerSupplier, int batchSize,
                    Function<StreamEvent, T> eventTransform, AuthenticationContext authenticationContext,
                    AuthorizationEnforcer authorizationEnforcer) {
    this.streamId = streamId;
    this.consumerSupplier = consumerSupplier;
    this.batchSize = batchSize;
    this.eventTransform = eventTransform;
    this.authorizationEnforcer = authorizationEnforcer;
    this.principal = authenticationContext.getPrincipal();
  }

  @Override
  public InputDatum<T> dequeue(long timeout, TimeUnit timeoutUnit) throws IOException, InterruptedException {
    try {
      // Ensure that the user has READ permission to access the stream
      authorizationEnforcer.enforce(streamId, principal, Action.READ);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw new IOException(e);
    }
    StreamConsumer consumer = consumerSupplier.get();
    return new BasicInputDatum<>(QueueName.fromStream(consumer.getStreamId()),
                                 consumer.poll(batchSize, timeout, timeoutUnit), eventTransform);
  }
}
