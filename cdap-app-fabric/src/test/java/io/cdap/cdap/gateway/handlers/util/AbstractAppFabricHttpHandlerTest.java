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

package io.cdap.cdap.gateway.handlers.util;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.internal.MockResponder;
import io.cdap.cdap.proto.Instances;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.twill.api.logging.LogEntry;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AbstractAppFabricHttpHandler}.
 */
public class AbstractAppFabricHttpHandlerTest {

  private static FullHttpRequest createRequest(String body) {
    return new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "/v3/test",
        body == null
            ? Unpooled.EMPTY_BUFFER
            : Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
  }

  @Test
  public void testParseBodyValidEmptyAndMalformedJson() throws Exception {
    TestAppFabricHttpHandler handler = new TestAppFabricHttpHandler();

    // Empty body returns null for parseBody and empty map for decodeArguments.
    Assert.assertNull(handler.parse(createRequest(null), Instances.class));
    Assert.assertEquals(Collections.emptyMap(), handler.decode(createRequest(null)));

    // Valid JSON body parses Instances, Map, and EntityId type adapter.
    Assert.assertEquals(4, handler.readInstances(createRequest("{\"instances\":4}")));
    Assert.assertEquals(
        ImmutableMap.of("k", "v"),
        handler.decode(createRequest("{\"k\":\"v\"}")));
    EntityId parsedEntity = handler.parse(
        createRequest("{\"entity\":\"NAMESPACE\",\"namespace\":\"ns1\"}"), EntityId.class);
    Assert.assertEquals(new NamespaceId("ns1"), parsedEntity);

    // Invalid JSON throws JsonSyntaxException from parseBody and BadRequestException from getInstances.
    Assert.assertThrows(JsonSyntaxException.class,
        () -> handler.parse(createRequest("{not-valid-json"), Instances.class));
    Assert.assertThrows(BadRequestException.class,
        () -> handler.readInstances(createRequest("{not-valid-json")));
    Assert.assertThrows(BadRequestException.class,
        () -> handler.readInstances(createRequest(null)));
  }

  @Test
  public void testHelperMethodsRespondIfElementNotFoundAndTransformLogLevels() {
    TestAppFabricHttpHandler handler = new TestAppFabricHttpHandler();

    MockResponder notFoundResponder = new MockResponder();
    Assert.assertTrue(handler.respondNotFound(
        new RuntimeException(new NoSuchElementException("missing")), notFoundResponder));
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND, notFoundResponder.getStatus());

    MockResponder otherResponder = new MockResponder();
    Assert.assertFalse(handler.respondNotFound(
        new IllegalStateException("other"), otherResponder));

    Map<String, LogEntry.Level> transformed =
        handler.transformLevels(ImmutableMap.of("root", "debug", "app", "WARN"));
    Assert.assertEquals(LogEntry.Level.DEBUG, transformed.get("root"));
    Assert.assertEquals(LogEntry.Level.WARN, transformed.get("app"));
  }

  private static final class TestAppFabricHttpHandler extends AbstractAppFabricHttpHandler {

    @Nullable
    private <T> T parse(FullHttpRequest request, Type type) {
      return parseBody(request, type);
    }

    private int readInstances(FullHttpRequest request) throws BadRequestException {
      return getInstances(request);
    }

    private Map<String, String> decode(FullHttpRequest request) {
      return decodeArguments(request);
    }

    private boolean respondNotFound(Throwable t, MockResponder responder) {
      return respondIfElementNotFound(t, responder);
    }

    private Map<String, LogEntry.Level> transformLevels(Map<String, String> levels) {
      return transformLogLevelsMap(levels);
    }
  }
}
