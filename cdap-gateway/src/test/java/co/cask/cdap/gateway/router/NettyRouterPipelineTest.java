/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.gateway.apps.AppWritingtoStream;
import co.cask.cdap.internal.test.AppJarHelper;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import com.ning.http.client.providers.netty.NettyAsyncHttpProvider;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Verify the ordering of events in the RouterPipeline.
 */
public class NettyRouterPipelineTest {

  private static final Logger LOG = LoggerFactory.getLogger(NettyRouterPipelineTest.class);
  private static final String HOSTNAME = "127.0.0.1";
  private static final int MAX_UPLOAD_BYTES = 10 * 1024 * 1024;

  private static final String GATEWAY_NAME = Constants.Router.GATEWAY_DISCOVERY_NAME;
  private static final String SERVICE_NAME = Constants.Service.APP_FABRIC_HTTP;

  private static final DiscoveryService DISCOVERY_SERVICE = new InMemoryDiscoveryService();
  public static final RouterResource ROUTER = new RouterResource(HOSTNAME, DISCOVERY_SERVICE);
  public static final ServerResource GATEWAY_SERVER = new ServerResource(HOSTNAME, DISCOVERY_SERVICE, SERVICE_NAME);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @SuppressWarnings("UnusedDeclaration")
  @ClassRule
  public static TestRule chain = RuleChain.outerRule(ROUTER).around(GATEWAY_SERVER);

  @Before
  public void clearNumRequests() throws Exception {
    GATEWAY_SERVER.clearNumRequests();

    // Wait for both servers of gatewayService to be registered
    Iterable<Discoverable> discoverables = ((DiscoveryServiceClient) DISCOVERY_SERVICE).discover(SERVICE_NAME);
    for (int i = 0; i < 50 && Iterables.size(discoverables) != 1; ++i) {
      TimeUnit.MILLISECONDS.sleep(50);
    }
  }

  @Test
  public void testChunkRequestSuccess() throws Exception {

    AsyncHttpClientConfig.Builder configBuilder = new AsyncHttpClientConfig.Builder();

    final AsyncHttpClient asyncHttpClient = new AsyncHttpClient(
      new NettyAsyncHttpProvider(configBuilder.build()),
      configBuilder.build());

    byte [] requestBody = generatePostData();
    final Request request = new RequestBuilder("POST")
      .setUrl(String.format("http://%s:%d%s", HOSTNAME, ROUTER.getServiceMap().get(GATEWAY_NAME), "/v1/upload"))
      .setContentLength(requestBody.length)
      .setBody(new ByteEntityWriter(requestBody))
      .build();

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Future<Void> future = asyncHttpClient.executeRequest(request, new AsyncCompletionHandler<Void>() {
      @Override
      public Void onCompleted(Response response) throws Exception {
        return null;
      }

      @Override
      public STATE onBodyPartReceived(HttpResponseBodyPart content) throws Exception {
        //TimeUnit.MILLISECONDS.sleep(RANDOM.nextInt(10));
        content.writeTo(byteArrayOutputStream);
        return super.onBodyPartReceived(content);
      }
    });

    future.get();
    Assert.assertArrayEquals(requestBody, byteArrayOutputStream.toByteArray());
  }

  @Test
  public void testDeployNTimes() throws Exception {
    // regression tests for race condition during multiple deploys.
    deploy(100);
  }

  //Deploy word count app n times.
  private void deploy(int num) throws Exception {

    String path = String.format("http://%s:%d/v1/deploy",
                                HOSTNAME, ROUTER.getServiceMap().get(GATEWAY_NAME));

    LocationFactory lf = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location programJar = AppJarHelper.createDeploymentJar(lf, AppWritingtoStream.class);
    GATEWAY_SERVER.setExpectedJarBytes(ByteStreams.toByteArray(Locations.newInputSupplier(programJar)));

    for (int i = 0; i < num; i++) {
      LOG.info("Deploying {}/{}", i, num);
      URL url = new URL(path);
      HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
      urlConn.setRequestProperty("X-Archive-Name", "Purchase-1.0.0.jar");
      urlConn.setRequestMethod("POST");
      urlConn.setDoOutput(true);
      urlConn.setDoInput(true);

      ByteStreams.copy(Locations.newInputSupplier(programJar), urlConn.getOutputStream());
      Assert.assertEquals(200, urlConn.getResponseCode());
      urlConn.getInputStream().close();
      urlConn.disconnect();
    }
  }

  private static class ByteEntityWriter implements Request.EntityWriter {
    private final byte [] bytes;

    private ByteEntityWriter(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public void writeEntity(OutputStream out) throws IOException {
      for (int i = 0; i < MAX_UPLOAD_BYTES; i += ServerResource.CHUNK_SIZE) {
        out.write(bytes, i, ServerResource.CHUNK_SIZE);
      }
    }
  }

  private static byte [] generatePostData() {
    byte [] bytes = new byte [MAX_UPLOAD_BYTES];

    for (int i = 0; i < MAX_UPLOAD_BYTES; ++i) {
      bytes[i] = (byte) i;
    }

    return bytes;
  }

}
