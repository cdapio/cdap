/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.common.http;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Unit test for {@link SpillableBodyConsumer}.
 */
public class SpillableBodyConsumerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testSpilling() throws Exception {
    testPost(Strings.repeat("0123456789", 10), 15);
  }

  @Test
  public void testNoSpill() throws Exception {
    testPost(Strings.repeat("0123456789", 10), 1024);
  }

  private void testPost(String body, int bufferLimit) throws Exception {
    NettyHttpService httpService = NettyHttpService.builder("test")
      .setHttpHandlers(new TestHandler(bufferLimit))
      .build();
    httpService.start();
    try {
      InetSocketAddress addr = httpService.getBindAddress();
      URL url = new URL(String.format("http://%s:%d/post", addr.getHostName(), addr.getPort()));
      HttpResponse response = HttpRequests.execute(io.cdap.common.http.HttpRequest.post(url).withBody(body).build(),
                                                   new HttpRequestConfig(1000, 10000000));

      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals(body, response.getResponseBodyAsString());
    } finally {
      httpService.stop();
    }
  }

  /**
   * Handler for testing only.
   */
  public static final class TestHandler extends AbstractHttpHandler {

    private final int bufferSize;

    public TestHandler(int bufferSize) {
      this.bufferSize = bufferSize;
    }

    @POST
    @Path("/post")
    public BodyConsumer post(HttpRequest request, HttpResponder responder) throws IOException {
      return new SpillableBodyConsumer(TEMP_FOLDER.newFile().toPath(), bufferSize) {
        @Override
        protected void processInput(InputStream is, HttpResponder responder) throws IOException {
          responder.sendByteArray(HttpResponseStatus.OK, ByteStreams.toByteArray(is), new DefaultHttpHeaders());
        }
      };
    }
  }
}
