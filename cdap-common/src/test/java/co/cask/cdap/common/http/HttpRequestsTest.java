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

package co.cask.cdap.common.http;

import co.cask.http.NettyHttpService;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 */
public class HttpRequestsTest extends HttpRequestsTestBase {

  private static TestHttpService httpService;

  @Before
  public void setUp() {
    httpService = new TestHttpService();
    httpService.startAndWait();
  }

  @After
  public void tearDown() {
    httpService.stopAndWait();
  }

  @Override
  protected URI getBaseURI() throws URISyntaxException {
    InetSocketAddress bindAddress = httpService.getBindAddress();
    return new URI("http://" + bindAddress.getHostName() + ":" + bindAddress.getPort());
  }

  @Override
  protected HttpRequestConfig getHttpRequestsConfig() {
    return HttpRequestConfig.DEFAULT;
  }

  public static final class TestHttpService extends AbstractIdleService {

    private final NettyHttpService httpService;

    public TestHttpService() {
      this.httpService = NettyHttpService.builder()
        .setHost("localhost")
        .addHttpHandlers(Sets.newHashSet(new HttpRequestsTestBase.TestHandler()))
        .setWorkerThreadPoolSize(10)
        .setExecThreadPoolSize(10)
        .setConnectionBacklog(20000)
        .build();
    }

    public InetSocketAddress getBindAddress() {
      return httpService.getBindAddress();
    }

    @Override
    protected void startUp() throws Exception {
      httpService.startAndWait();
    }

    @Override
    protected void shutDown() throws Exception {
      httpService.stopAndWait();
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("bindAddress", httpService.getBindAddress())
        .toString();
    }
  }
}
