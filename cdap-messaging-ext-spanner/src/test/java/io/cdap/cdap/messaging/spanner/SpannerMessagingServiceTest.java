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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.spi.MessagingServiceContext;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for Cloud spanner implementation of the
 * {@link io.cdap.cdap.messaging.spi.MessagingService}. This test needs the following Java
 * properties to run. If they are not provided, tests will be ignored.
 *
 * <ul>
 *   <li>gcp.project - GCP project name</li>
 *   <li>gcp.spanner.instance - GCP spanner instance name</li>
 *   <li>gcp.spanner.database - GCP spanner database name</li>
 *   <li>(optional) gcp.credentials.path - Local file path to the service account
 *   json that has the "Cloud Spanner Database User" role</li>
 * </ul>
 */
public class SpannerMessagingServiceTest {

  private static SpannerMessagingService service;

  private static SpannerTopicMetadata SIMPLE_TOPIC = new SpannerTopicMetadata(
      new TopicId("system", "topic1"), new HashMap<>());

  private static SpannerTopicMetadata TOPIC_WITH_PROPERTIES = new SpannerTopicMetadata(
      new TopicId("system", "topic2"), new HashMap<String, String>() {
    {
      put("key1", "value1");
      put("key2", "value2");
    }
  });

  @BeforeClass
  public static void createSpannerMessagingService() throws Exception {
    String project = System.getProperty("gcp.project");
    String instance = System.getProperty("gcp.spanner.instance");
    String database = System.getProperty("gcp.spanner.database");
    String credentialsPath = System.getProperty("gcp.credentials.path");

    // GCP project, instance, and database must be provided
    Assume.assumeNotNull(project, instance, database);

    Map<String, String> configs = new HashMap<>();
    configs.put(SpannerUtil.PROJECT, project);
    configs.put(SpannerUtil.INSTANCE, instance);
    configs.put(SpannerUtil.DATABASE, database);

    if (credentialsPath != null) {
      configs.put(SpannerUtil.CREDENTIALS_PATH, credentialsPath);
    }

    MessagingServiceContext context = new MockMessagingServiceContext(configs);

    service = new SpannerMessagingService();
    service.initialize(context);
  }

  @After
  public void cleanUp() throws Exception {
    try {
      service.deleteTopic(SIMPLE_TOPIC.getTopicId());
      service.deleteTopic(TOPIC_WITH_PROPERTIES.getTopicId());
    } catch (TopicNotFoundException e) {
      // no-op
    }
  }

  @Test
  public void testCreateTopic() throws Exception {
    service.createTopic(SIMPLE_TOPIC);
    Assert.assertEquals(SIMPLE_TOPIC.getProperties(),
        service.getTopicMetadataProperties(SIMPLE_TOPIC.getTopicId()));
  }

  @Test
  public void testCreateTopicWithProperties() throws Exception {
    service.createTopic(TOPIC_WITH_PROPERTIES);
    Assert.assertEquals(TOPIC_WITH_PROPERTIES.getProperties(),
        service.getTopicMetadataProperties(TOPIC_WITH_PROPERTIES.getTopicId()));
  }

  @Test(expected = TopicNotFoundException.class)
  public void testGetMetadataProperties() throws Exception {
    TopicId topicId = new TopicId("system", "invalid");
    service.getTopicMetadataProperties(topicId);
  }

  @Test
  public void testListTopics() throws Exception {
    service.createTopic(SIMPLE_TOPIC);
    service.createTopic(TOPIC_WITH_PROPERTIES);
    List<TopicId> topics = service.listTopics(new NamespaceId("system"));
    Assert.assertEquals(new ArrayList<>(Arrays.asList(
        new TopicId("system", SpannerMessagingService.getTableName(SIMPLE_TOPIC.getTopicId())),
        new TopicId("system",
            SpannerMessagingService.getTableName(TOPIC_WITH_PROPERTIES.getTopicId())))), topics);
  }

  @Test
  public void testListTopicsEmptyNamespace() throws Exception {
    List<TopicId> topics = service.listTopics(new NamespaceId("namespace"));
    Assert.assertEquals(new ArrayList<>(), topics);
  }

  private static final class MockMessagingServiceContext implements MessagingServiceContext {

    private final Map<String, String> config;

    MockMessagingServiceContext(Map<String, String> config) {
      this.config = config;
    }

    @Override
    public Map<String, String> getProperties() {
      return config;
    }
  }

  private static final class SpannerTopicMetadata implements TopicMetadata {

    private final TopicId topicId;
    private final Map<String, String> properties;

    public SpannerTopicMetadata(TopicId topicId, Map<String, String> properties) {
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

}