/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.internal.app.runtime.service.http;

import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.service.http.HttpServiceContext;
import com.continuuity.api.service.http.HttpServiceHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.http.NettyHttpService;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.apache.twill.discovery.ServiceDiscovered;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 *
 */
public class HttpHandlerGeneratorTest {

  @Path("/v2")
  public static final class MyHttpHandler implements HttpServiceHandler {

    @GET
    @Path("/handle")
    public void process(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "Hello World");
    }

    @Override
    public void initialize(HttpServiceContext context) throws Exception {

    }

    @Override
    public void destroy() {

    }
  }

  @Test
  public void test() throws Exception {
    HttpHandlerFactory factory = new HttpHandlerFactory();
    HttpHandler httpHandler = factory.createHttpHandler(new MyHttpHandler(), new HttpServiceContext() {
      @Override
      public Map<String, String> getRuntimeArguments() {
        return null;
      }

      @Override
      public ServiceDiscovered discover(String applicationId, String serviceId, String serviceName) {
        return null;
      }

      @Override
      public <T extends Closeable> T getDataSet(String name) throws DataSetInstantiationException {
        return null;
      }
    });

    NettyHttpService service = NettyHttpService.builder().addHttpHandlers(ImmutableList.of(httpHandler)).build();
    service.startAndWait();

    InetSocketAddress bindAddress = service.getBindAddress();
    URLConnection urlConn = new URL(String.format("http://%s:%d/v2/handle",
                                                  bindAddress.getHostName(), bindAddress.getPort())).openConnection();
    try {
      Assert.assertEquals("Hello World", new String(ByteStreams.toByteArray(urlConn.getInputStream()), Charsets.UTF_8));
    } finally {
      service.stopAndWait();
    }
  }
}
