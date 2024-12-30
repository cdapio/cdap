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
package io.cdap.cdap.messaging.spanner;

import static io.cdap.cdap.messaging.spanner.SpannerMessagingServiceTest.SYSTEM_TOPICS;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.messaging.spi.MessageFetchRequest;
import io.cdap.cdap.messaging.spi.MessagingServiceContext;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.proto.id.TopicId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.tephra.Transaction;

/**
 * This is a helper class which provides implementation for most of the abstract classes used in
 * {@link SpannerMessagingServiceTest}. As part of [CDAP-21090], most of the classes can be removed
 * from here as the default implementations would be moved out of the cdap-tms module.
 */
public class SpannerMessagingServiceTestUtil {

  static final class MockMessagingServiceContext implements MessagingServiceContext {

    private final Map<String, String> config;

    MockMessagingServiceContext(Map<String, String> config) {
      this.config = config;
    }

    @Override
    public Map<String, String> getProperties() {
      return config;
    }

    @Override
    public List<TopicMetadata> getSystemTopics() {
      return SYSTEM_TOPICS;
    }
  }

  static final class SpannerTopicMetadata implements TopicMetadata {

    private final TopicId topicId;
    private final Map<String, String> properties;

    SpannerTopicMetadata(TopicId topicId, Map<String, String> properties) {
      this.topicId = topicId;
      this.properties = ImmutableMap.copyOf(properties);
    }

    @Override
    public TopicId getTopicId() {
      return topicId;
    }

    @Override
    public Map<String, String> getProperties() {
      return properties;
    }

    @Override
    public int getGeneration() {
      return 0;
    }

    @Override
    public boolean exists() {
      return false;
    }

    @Override
    public long getTTL() {
      return 0;
    }
  }

  static final class SpannerStoreRequest implements StoreRequest {

    private final TopicId topicId;
    private final List<String> payloads;

    SpannerStoreRequest(TopicId topicId, List<String> payloads) {
      this.topicId = topicId;
      this.payloads = payloads;
    }

    @Override
    public TopicId getTopicId() {
      return topicId;
    }

    @Override
    public boolean isTransactional() {
      return false;
    }

    @Override
    public long getTransactionWritePointer() {
      return 0;
    }

    @Override
    public boolean hasPayload() {
      return false;
    }

    @Override
    public Iterator<byte[]> iterator() {
      return payloads.stream().map(Bytes::toBytes).iterator();
    }
  }

  static final class SpannerMessageFetchRequest implements MessageFetchRequest {

    private final TopicId topicId;
    private final byte[] startOffset;
    private final int limit;

    SpannerMessageFetchRequest(TopicId topicId, byte[] startOffset, int limit) {
      this.topicId = topicId;
      this.startOffset = startOffset;
      this.limit = limit;
    }

    @Override
    public TopicId getTopicId() {
      return topicId;
    }

    @Nullable
    @Override
    public byte[] getStartOffset() {
      return startOffset;
    }

    @Override
    public boolean isIncludeStart() {
      return false;
    }

    @Nullable
    @Override
    public Long getStartTime() {
      return null;
    }

    @Nullable
    @Override
    public Transaction getTransaction() {
      return null;
    }

    @Override
    public int getLimit() {
      return limit;
    }
  }

}
