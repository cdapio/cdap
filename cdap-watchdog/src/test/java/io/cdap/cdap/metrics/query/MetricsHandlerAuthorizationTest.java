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

package io.cdap.cdap.metrics.query;

import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests authorization of the metrics query, search, and processor status endpoints in
 * {@link MetricsHandler}.
 */
public class MetricsHandlerAuthorizationTest {

  private static final NamespaceId DENIED = new NamespaceId("denied");

  private MetricsQueryHelper queryHelper;
  private ContextAccessEnforcer enforcer;
  private HttpResponder responder;
  private MetricsHandler handler;

  @Before
  public void setUp() {
    queryHelper = Mockito.mock(MetricsQueryHelper.class);
    enforcer = Mockito.mock(ContextAccessEnforcer.class);
    responder = Mockito.mock(HttpResponder.class);
    handler = new MetricsHandler(queryHelper, enforcer);
    Mockito.doThrow(new UnauthorizedException("denied"))
        .when(enforcer).enforce(DENIED, StandardPermission.GET);
    Mockito.doThrow(new UnauthorizedException("denied"))
        .when(enforcer).enforce(InstanceId.SELF, StandardPermission.UPDATE);
  }

  @Test
  public void testSearch() throws Exception {
    handler.search(null, responder, "metric", Collections.singletonList("namespace:allowed"));
    Mockito.verify(queryHelper).searchMetric(Collections.singletonList("namespace:allowed"));

    assertUnauthorized(() -> handler.search(null, responder, "tag",
        Arrays.asList("namespace:allowed", "ns:denied")));
    assertUnauthorized(() -> handler.search(null, responder, "tag",
        Collections.singletonList("namespace:*")));
    assertUnauthorized(() -> handler.search(null, responder, "tag",
        Collections.singletonList("namespace:system")));
    assertUnauthorized(() -> handler.search(null, responder, "tag",
        Collections.singletonList("app:myapp")));
    Mockito.verify(queryHelper, Mockito.never()).searchTags(Mockito.any());
  }

  @Test
  public void testTagQuery() throws Exception {
    List<String> allowedTags = Collections.singletonList("namespace:allowed");
    handler.query(createRequest("/v3/metrics/query?tag=namespace:allowed", null), responder,
        Collections.emptyList(), Collections.emptyList(), allowedTags);
    Mockito.verify(queryHelper).executeTagQuery(Mockito.eq(allowedTags), Mockito.any(),
        Mockito.any(), Mockito.any());

    assertUnauthorized(() -> handler.query(createRequest("/v3/metrics/query?tag=ns:denied", null),
        responder, Collections.emptyList(), Collections.emptyList(),
        Collections.singletonList("ns:denied")));
    assertUnauthorized(() -> handler.query(createRequest("/v3/metrics/query?tag=ns:system", null),
        responder, Collections.emptyList(), Collections.emptyList(),
        Collections.singletonList("ns:system")));
    assertUnauthorized(() -> handler.query(createRequest("/v3/metrics/query?metric=m", null),
        responder, Collections.singletonList("m"), Collections.emptyList(),
        Collections.emptyList()));
    Mockito.verify(queryHelper).executeTagQuery(Mockito.any(), Mockito.any(), Mockito.any(),
        Mockito.any());
  }

  @Test
  public void testBatchQuery() throws Exception {
    handler.query(createRequest("/v3/metrics/query",
        "{\"q1\": {\"tags\": {\"namespace\": \"allowed\"}, \"metrics\": [\"m\"]}}"), responder,
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    Mockito.verify(queryHelper).executeBatchQueries(Mockito.any());

    assertUnauthorized(() -> handler.query(createRequest("/v3/metrics/query",
        "{\"q1\": {\"tags\": {\"namespace\": \"allowed\"}, \"metrics\": [\"m\"]},"
            + " \"q2\": {\"tags\": {\"namespace\": \"system\"}, \"metrics\": [\"m\"]}}"),
        responder, Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
    assertUnauthorized(() -> handler.query(createRequest("/v3/metrics/query",
        "{\"q1\": {\"tags\": {\"app\": \"myapp\"}, \"metrics\": [\"m\"]}}"),
        responder, Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
    Mockito.verify(queryHelper, Mockito.times(1)).executeBatchQueries(Mockito.any());
  }

  @Test
  public void testProcessorStatus() throws Exception {
    MetricStore metricStore = Mockito.mock(MetricStore.class);
    Mockito.when(queryHelper.getMetricStore()).thenReturn(metricStore);

    assertUnauthorized(() -> handler.processorStatus(null, responder));
    Mockito.verify(metricStore, Mockito.never()).getMetricsProcessorStats();

    Mockito.reset(enforcer);
    handler.processorStatus(null, responder);
    Mockito.verify(enforcer).enforce(InstanceId.SELF, StandardPermission.UPDATE);
    Mockito.verify(responder).sendJson(Mockito.eq(HttpResponseStatus.OK), Mockito.anyString());
  }

  private static FullHttpRequest createRequest(String uri, String body) {
    DefaultFullHttpRequest request = body == null
        ? new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri)
        : new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri,
            Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
    request.headers().set(HttpHeaderNames.CONTENT_LENGTH, request.content().readableBytes());
    return request;
  }

  private static void assertUnauthorized(ThrowingRunnable runnable) throws Exception {
    try {
      runnable.run();
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
