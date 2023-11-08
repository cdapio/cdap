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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.GcpMetadataTaskContext;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import java.net.URL;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Tests for {@link GcpMetadataHttpHandlerInternal}.
 */
public class GcpMetadataHttpHandlerInternalTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final Gson GSON =
      ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static NettyHttpService httpService;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    NamespaceAdmin namespaceAdmin = new InMemoryNamespaceAdmin();
    namespaceAdmin.create(NamespaceMeta.SYSTEM);
    namespaceAdmin.create(NamespaceMeta.DEFAULT);

    RemoteClientFactory remoteClientFactory = Mockito.mock(RemoteClientFactory.class);
    httpService = new CommonNettyHttpServiceBuilder(cConf, "test",
        new NoOpMetricsCollectionService())
        .setHttpHandlers(
            new GcpMetadataHttpHandlerInternal(cConf, remoteClientFactory,
                new NoOpRemoteAuthenticator())
        )
        .setChannelPipelineModifier(new ChannelPipelineModifier() {
          @Override
          public void modify(ChannelPipeline pipeline) {
            pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
          }
        })
        .build();
    httpService.start();
  }

  @AfterClass
  public static void finish() throws Exception {
    httpService.stop();
  }

  @Test
  public void testStatus() throws Exception {
    String endpoint = String.format("http://%s:%s/",
        httpService.getBindAddress().getHostName(), httpService.getBindAddress().getPort());
    URL url = new URL(endpoint);
    HttpRequest httpRequest = HttpRequest.get(url).build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(403, httpResponse.getResponseCode());

    httpRequest = HttpRequest.get(url).addHeader(
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE).build();
    httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(200, httpResponse.getResponseCode());
  }

  @Test
  public void testSetAndClearContext() throws Exception {
    String endpoint = String.format("http://%s:%s",
        httpService.getBindAddress().getHostName(), httpService.getBindAddress().getPort());
    NamespaceId namespaceId = new NamespaceId("test");
    GcpMetadataTaskContext gcpMetadataTaskContext =
        new GcpMetadataTaskContext(namespaceId.getNamespace(),
            "alice", "0.0.0.0", null);

    // set context
    URL url = new URL(String.format("%s/set-context", endpoint));
    HttpRequest httpRequest = HttpRequest.put(url).withBody(GSON.toJson(gcpMetadataTaskContext,
        GcpMetadataTaskContext.class)).build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(200, httpResponse.getResponseCode());
    String expectedResponse = String.format("Context was set successfully with namespace '%s'.",
        namespaceId.getNamespace());
    Assert.assertEquals(expectedResponse, httpResponse.getResponseBodyAsString());

    //clear context
    url = new URL(String.format("%s/clear-context", endpoint));
    httpRequest = HttpRequest.delete(url).build();
    httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(200, httpResponse.getResponseCode());
  }
}
