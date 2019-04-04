/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.messaging.client;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import io.cdap.cdap.messaging.StoreRequest;
import io.cdap.cdap.proto.id.TopicId;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * A builder to create {@link StoreRequest} instances. This is mainly used on the client side.
 */
public final class StoreRequestBuilder {

  private static final Function<String, byte[]> STRING_TO_BYTES = input -> input.getBytes(StandardCharsets.UTF_8);

  private final TopicId topicId;
  private List<byte[]> payloads;
  private Long txWritePointer;

  /**
   * Creates a new {@link StoreRequestBuilder} instance.
   *
   * @param topicId the topic that the store action will happen.
   */
  public static StoreRequestBuilder of(TopicId topicId) {
    return new StoreRequestBuilder(topicId);
  }

  /**
   * Constructor. This is private and the {@link #of(TopicId)} method should be used.
   */
  private StoreRequestBuilder(TopicId topicId) {
    this.topicId = topicId;
  }

  /**
   * Adds a single payload string to the request. The string will be converted to byte arrays using UTF-8 encoding.
   */
  public StoreRequestBuilder addPayload(String payload) {
    return addPayload(STRING_TO_BYTES.apply(payload));
  }

  /**
   * Adds a single byte array to the payload of the request.
   */
  public StoreRequestBuilder addPayload(byte[] payload) {
    getPayloads().add(payload);
    return this;
  }

  /**
   * Adds a list of byte arrays as the payloads of the request.
   */
  public StoreRequestBuilder addPayloads(Iterator<byte[]> payloads) {
    Iterators.addAll(getPayloads(), payloads);
    return this;
  }
  
  /**
   * Adds a list of byte arrays as the payloads of the request.
   */
  public StoreRequestBuilder addPayloads(Iterable<byte[]> payloads) {
    Iterables.addAll(getPayloads(), payloads);
    return this;
  }

  /**
   * Sets the transaction write pointer to be used for the request.
   *
   * @param txWritePointer the transaction write pointer if want to publish transactionally, or {@code null}
   *                       for non-transactional publish.
   */
  public StoreRequestBuilder setTransaction(@Nullable Long txWritePointer) {
    this.txWritePointer = txWritePointer;
    return this;
  }

  /**
   * Returns {@code true} if there is some payload in this builder.
   */
  public boolean hasPayload() {
    return payloads != null && !payloads.isEmpty();
  }

  /**
   * Creates a {@link StoreRequest} based on the settings in this builder.
   */
  public StoreRequest build() {
    if (txWritePointer == null && (payloads == null || payloads.isEmpty())) {
      throw new IllegalArgumentException("Payload cannot be empty for non-transactional publish");
    }
    return new SimpleStoreRequest(topicId, txWritePointer != null, txWritePointer == null ? -1L : txWritePointer,
                                  payloads);
  }

  /**
   * Returns the payloads {@link List} used by this builder.
   */
  private List<byte[]> getPayloads() {
    if (payloads != null) {
      return payloads;
    }
    payloads = new LinkedList<>();
    return payloads;
  }

  /**
   * A straightforward implementation of {@link StoreRequest}.
   */
  private static final class SimpleStoreRequest extends StoreRequest {

    private final List<byte[]> payloads;

    SimpleStoreRequest(TopicId topicId, boolean transactional, long transactionWritePointer,
                       @Nullable List<byte[]> payloads) {
      super(topicId, transactional, transactionWritePointer);
      this.payloads = payloads == null ? Collections.emptyList() : payloads;
    }

    @Override
    public boolean hasPayload() {
      return !payloads.isEmpty();
    }

    @Override
    public Iterator<byte[]> iterator() {
      return payloads.iterator();
    }
  }
}
