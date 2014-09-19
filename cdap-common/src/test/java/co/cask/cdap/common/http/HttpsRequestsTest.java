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
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 *
 */
public class HttpsRequestsTest extends HttpRequestsTestBase {

  private static TestHttpsService httpsService;

  @Before
  public void setUp() throws Exception {
    httpsService = new TestHttpsService();
    httpsService.startAndWait();
  }

  @After
  public void tearDown() {
    httpsService.stopAndWait();
  }

  @Override
  protected URI getBaseURI() throws URISyntaxException {
    InetSocketAddress bindAddress = httpsService.getBindAddress();
    return new URI("https://" + bindAddress.getHostName() + ":" + bindAddress.getPort());
  }

  @Override
  protected HttpRequestConfig getHttpRequestsConfig() {
    return new HttpRequestConfig(0, 0, true);
  }

  public static final class TestHttpsService extends AbstractIdleService {

    private final NettyHttpService httpService;

    public TestHttpsService() throws URISyntaxException {
      URL keystore = getClass().getClassLoader().getResource("cert.jks");
      Assert.assertNotNull(keystore);

      this.httpService = NettyHttpService.builder()
        .setHost("localhost")
        .addHttpHandlers(Sets.newHashSet(new TestHandler()))
        .setWorkerThreadPoolSize(10)
        .setExecThreadPoolSize(10)
        .setConnectionBacklog(20000)
        .enableSSL(new File(keystore.toURI()), "secret", "secret")
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
