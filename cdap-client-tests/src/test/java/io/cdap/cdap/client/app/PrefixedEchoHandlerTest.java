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

package io.cdap.cdap.client.app;

import io.cdap.cdap.api.service.http.HttpServiceContext;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link PrefixedEchoHandler}.
 */
public class PrefixedEchoHandlerTest {

  @Test
  public void testInitializeWithPresentAndAbsentRuntimeArgument() throws Exception {
    PrefixedEchoHandler withPrefix = new PrefixedEchoHandler();
    withPrefix.initialize(createServiceContext(Collections.singletonMap("sdf", "myPrefix")));
    Assert.assertEquals("myPrefix:payload", invokeEcho(withPrefix, "payload"));

    PrefixedEchoHandler withoutPrefix = new PrefixedEchoHandler();
    withoutPrefix.initialize(createServiceContext(Collections.emptyMap()));
    Assert.assertEquals(":payload", invokeEcho(withoutPrefix, "payload"));
  }

  private String invokeEcho(PrefixedEchoHandler handler, String body) {
    HttpServiceRequest request = (HttpServiceRequest) Proxy.newProxyInstance(
        HttpServiceRequest.class.getClassLoader(),
        new Class<?>[]{HttpServiceRequest.class},
        (proxy, method, args) -> {
          if ("getContent".equals(method.getName())) {
            return ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8));
          }
          return null;
        });

    AtomicInteger status = new AtomicInteger();
    AtomicReference<String> responseBody = new AtomicReference<>();
    HttpServiceResponder responder = (HttpServiceResponder) Proxy.newProxyInstance(
        HttpServiceResponder.class.getClassLoader(),
        new Class<?>[]{HttpServiceResponder.class},
        (proxy, method, args) -> {
          if ("sendString".equals(method.getName()) && args.length == 3) {
            status.set((Integer) args[0]);
            responseBody.set((String) args[1]);
          }
          return null;
        });

    handler.echo(request, responder);
    Assert.assertEquals(200, status.get());
    return responseBody.get();
  }

  private HttpServiceContext createServiceContext(Map<String, String> runtimeArgs) {
    return (HttpServiceContext) Proxy.newProxyInstance(
        HttpServiceContext.class.getClassLoader(),
        new Class<?>[]{HttpServiceContext.class},
        (proxy, method, args) -> {
          if ("getRuntimeArguments".equals(method.getName())) {
            return runtimeArgs;
          }
          return null;
        });
  }
}
