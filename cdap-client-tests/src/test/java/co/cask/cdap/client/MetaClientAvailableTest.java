/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ConnectException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Test for {@link MetaClient#ping()} when CDAP is unavailable.
 */
public class MetaClientAvailableTest {

  private TestPingHandler handler;
  private NettyHttpService service;
  private MetaClient metaClient;
  private MetaClient fakeMetaClient;

  @Before
  public void setUp() {
    handler = new TestPingHandler();
    service = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.of(handler))
      .build();
    service.startAndWait();

    metaClient = new MetaClient(
      ClientConfig.builder()
        .setConnectionConfig(
          ConnectionConfig.builder()
            .setHostname(service.getBindAddress().getHostName())
            .setPort(service.getBindAddress().getPort())
            .setSSLEnabled(false)
            .build())
        .build());

    fakeMetaClient = new MetaClient(
      ClientConfig.builder()
        .setConnectionConfig(
          ConnectionConfig.builder()
            .setHostname(service.getBindAddress().getHostName())
            .setPort(service.getBindAddress().getPort() + 1)
            .setSSLEnabled(false)
            .build())
        .build());
  }

  @After
  public void tearDown() {
    service.stopAndWait();
  }

  @Test
  public void testAvailable() throws IOException, UnauthenticatedException {
    handler.setResponse(HttpResponseStatus.OK, "OK.\n");
    metaClient.ping();
  }

  @Test
  public void testUnavailable() throws IOException, UnauthenticatedException {
    try {
      fakeMetaClient.ping();
      Assert.fail();
    } catch (ConnectException e) {
      // expected
    }
  }

  @Test
  public void testWrongCode() throws UnauthenticatedException {
    try {
      handler.setResponse(HttpResponseStatus.CONFLICT, "HI");
      metaClient.ping();
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(
        "Expected IOException message to contain '409: HI', but got: " + e.getMessage(),
        e.getMessage().contains("409: HI"));
    }
  }

  @Test
  public void testWrongBody() throws UnauthenticatedException {
    try {
      handler.setResponse(HttpResponseStatus.OK, "???");
      metaClient.ping();
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(
        "Expected IOException message to contain 'response body', but got: " + e.getMessage(),
        e.getMessage().contains("response body"));
    }
  }

  public final class TestPingHandler extends AbstractHttpHandler {

    private HttpResponseStatus status;
    private String body;

    @GET
    @Path("/ping")
    public void ping(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(status, body);
    }

    public void setResponse(HttpResponseStatus status, String body) {
      this.status = status;
      this.body = body;
    }
  }
}
