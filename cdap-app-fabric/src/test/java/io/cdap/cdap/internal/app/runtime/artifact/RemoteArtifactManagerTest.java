/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link RemoteArtifactManager}.
 */
public class RemoteArtifactManagerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .create();

  @Test
  public void testListArtifactsSuccess() throws Exception {
    ArtifactInfo expectedInfo = new ArtifactInfo(
        "my-artifact", "1.0.0", ArtifactScope.USER,
        ArtifactClasses.builder().build(), Collections.singletonMap("k", "v"));
    String body = GSON.toJson(Collections.singletonList(expectedInfo));
    HttpResponse response = Mockito.mock(HttpResponse.class);
    Mockito.when(response.getResponseCode()).thenReturn(200);
    Mockito.when(response.getResponseBodyAsString()).thenReturn(body);

    RemoteClient remoteClient = createMockRemoteClient();
    Mockito.when(remoteClient.execute(Mockito.any(HttpRequest.class))).thenReturn(response);

    RemoteArtifactManager manager = createManager(remoteClient);
    List<ArtifactInfo> artifacts = manager.listArtifacts();
    Assert.assertEquals(Collections.singletonList(expectedInfo), artifacts);
  }

  @Test
  public void testListArtifactsPropagatesCheckedExceptions() throws Exception {
    IOException ioException = new IOException("network failure");
    RemoteClient ioFailingClient = createMockRemoteClient();
    Mockito.when(ioFailingClient.execute(Mockito.any(HttpRequest.class))).thenThrow(ioException);

    RemoteArtifactManager ioManager = createManager(ioFailingClient);
    try {
      ioManager.listArtifacts("ns1");
      Assert.fail("Expected IOException");
    } catch (IOException thrown) {
      Assert.assertSame(ioException, thrown);
    }

    UnauthorizedException unauthorized = new UnauthorizedException("denied");
    RemoteClient authFailingClient = createMockRemoteClient();
    Mockito.when(authFailingClient.execute(Mockito.any(HttpRequest.class))).thenThrow(unauthorized);

    RemoteArtifactManager authManager = createManager(authFailingClient);
    try {
      authManager.listArtifacts("ns1");
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException thrown) {
      Assert.assertSame(unauthorized, thrown);
    }
  }

  @Test
  public void testListArtifactsPropagatesUncheckedExceptionUnwrapped() throws Exception {
    IllegalStateException expected = new IllegalStateException("unexpected client state");
    RemoteClient remoteClient = createMockRemoteClient();
    Mockito.when(remoteClient.execute(Mockito.any(HttpRequest.class))).thenThrow(expected);

    RemoteArtifactManager manager = createManager(remoteClient);
    try {
      manager.listArtifacts("ns1");
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(expected, thrown);
    }
  }

  private RemoteClient createMockRemoteClient() throws Exception {
    RemoteClient remoteClient = Mockito.mock(RemoteClient.class);
    Mockito.when(remoteClient.requestBuilder(Mockito.eq(HttpMethod.GET), Mockito.anyString()))
        .thenAnswer(invocation -> HttpRequest.get(
            new URL("http://localhost:1234/v3Internal/" + invocation.getArgument(1))));
    return remoteClient;
  }

  private RemoteArtifactManager createManager(RemoteClient remoteClient) throws IOException {
    RemoteClientFactory factory = Mockito.mock(RemoteClientFactory.class);
    Mockito.when(factory.createRemoteClient(
        Mockito.anyString(),
        Mockito.any(DefaultHttpRequestConfig.class),
        Mockito.anyString()))
        .thenReturn(remoteClient);

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    return new RemoteArtifactManager(
        cConf,
        new LocalLocationFactory(TEMP_FOLDER.newFolder()),
        NamespaceId.DEFAULT,
        (failureCount, startTime) -> -1L,
        factory,
        Optional.empty());
  }
}
