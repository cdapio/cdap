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

package co.cask.cdap.messaging.client;

import co.cask.cdap.messaging.StoreRequest;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * A builder to create {@link StoreRequest} instances. This is mainly used on the client side.
 */
public final class StoreRequestBuilder {

  private static final Function<String, byte[]> STRING_TO_BYTES = new Function<String, byte[]>() {
    @Override
    public byte[] apply(String input) {
      return input.getBytes(StandardCharsets.UTF_8);
    }
  };

  private final TopicId topicId;
  private Iterator<byte[]> payloads;
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
   * Adds a list of byte arrays as the payloads of the request.
   */
  public StoreRequestBuilder addPayloads(byte[]...payloads) {
    return addPayloads(Iterators.forArray(payloads));
  }

  /**
   * Adds a list of Strings as the payloads of the request. The Strings will be converted to byte arrays using
   * UTF-8 encoding.
   */
  public StoreRequestBuilder addPayloads(String...payloads) {
    return addPayloads(Iterators.transform(Iterators.forArray(payloads), STRING_TO_BYTES));
  }

  /**
   * Adds a list of byte arrays as the payloads of the request.
   */
  public StoreRequestBuilder addPayloads(Iterator<byte[]> payloads) {
    this.payloads = (this.payloads == null) ? payloads : Iterators.concat(this.payloads, payloads);
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
    return payloads != null && payloads.hasNext();
  }

  /**
   * Creates a {@link StoreRequest} based on the settings in this builder.
   */
  public StoreRequest build() {
    if (txWritePointer == null && payloads == null) {
      throw new IllegalArgumentException("Payload cannot be empty for non-transactional publish");
    }
    return new SimpleStoreRequest(topicId, txWritePointer != null, txWritePointer == null ? -1L : txWritePointer,
                                  payloads);
  }

  /**
   * A straightforward implementation of {@link StoreRequest}.
   */
  private static final class SimpleStoreRequest extends StoreRequest {

    private final Iterator<byte[]> payloads;

    SimpleStoreRequest(TopicId topicId, boolean transactional,
                       long transactionWritePointer, Iterator<byte[]> payloads) {
      super(topicId, transactional, transactionWritePointer);
      this.payloads = payloads;
    }

    @Nullable
    @Override
    protected byte[] doComputeNext() {
      if (payloads == null) {
        return null;
      }
      return payloads.hasNext() ? payloads.next() : null;
    }
  }
}
