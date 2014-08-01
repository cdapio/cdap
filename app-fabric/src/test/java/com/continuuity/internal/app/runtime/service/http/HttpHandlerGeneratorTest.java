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
import com.continuuity.api.service.http.HttpServiceRequest;
import com.continuuity.api.service.http.HttpServiceResponder;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.NettyHttpService;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class HttpHandlerGeneratorTest {

  @Path("/v1")
  public static class BaseHttpHandler implements HttpServiceHandler {

    @GET
    @Path("/handle")
    public void process(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendString("Hello World");
    }

    @Override
    public void initialize(HttpServiceContext context) throws Exception {

    }

    @Override
    public void destroy() {

    }
  }

  @Path("/v2")
  public static final class MyHttpHandler extends BaseHttpHandler {

    @Path("/echo/{name}")
    @POST
    public void echo(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("name") String name) {
      responder.sendString(Charsets.UTF_8.decode(request.getContent()).toString() + " " + name);
    }
  }

  @Test
  public void testHttpHandlerGenerator() throws Exception {
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
    try {
      InetSocketAddress bindAddress = service.getBindAddress();

      // Make a GET call
      URLConnection urlConn = new URL(String.format("http://%s:%d/v2/handle",
                                                    bindAddress.getHostName(), bindAddress.getPort())).openConnection();
      urlConn.setReadTimeout(2000);

      Assert.assertEquals("Hello World", new String(ByteStreams.toByteArray(urlConn.getInputStream()), Charsets.UTF_8));

      // Make a POST call
      urlConn = new URL(String.format("http://%s:%d/v2/echo/test",
                                      bindAddress.getHostName(), bindAddress.getPort())).openConnection();
      urlConn.setReadTimeout(2000);
      urlConn.setDoOutput(true);
      ByteStreams.copy(ByteStreams.newInputStreamSupplier("Hello".getBytes(Charsets.UTF_8)),
                       urlConn.getOutputStream());

      Assert.assertEquals("Hello test",
                          new String(ByteStreams.toByteArray(urlConn.getInputStream()), Charsets.UTF_8));
    } finally {
      service.stopAndWait();
    }
  }
}
