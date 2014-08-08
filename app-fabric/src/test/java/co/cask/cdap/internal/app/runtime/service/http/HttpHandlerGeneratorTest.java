/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.service.http.HttpServiceSpecification;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

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
  public abstract static class BaseHttpHandler extends AbstractHttpServiceHandler {

    @GET
    @Path("/handle")
    public void process(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendString("Hello World");
    }
  }

  @Path("/v2")
  public static final class MyHttpHandler extends BaseHttpHandler {

    @Path("/echo/{name}")
    @POST
    public void echo(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("name") String name) {
      responder.sendString(Charsets.UTF_8.decode(request.getContent()).toString() + " " + name);
    }

    @Override
    public void configure() {

    }
  }

  @Test
  public void testHttpHandlerGenerator() throws Exception {
    HttpHandlerFactory factory = new HttpHandlerFactory();
    HttpHandler httpHandler = factory.createHttpHandler(new MyHttpHandler(), new HttpServiceContext() {
      @Override
      public HttpServiceSpecification getSpecification() {
        return null;
      }

      @Override
      public Map<String, String> getRuntimeArguments() {
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
