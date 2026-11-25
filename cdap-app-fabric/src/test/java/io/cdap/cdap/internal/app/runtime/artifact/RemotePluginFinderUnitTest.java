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
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.api.retry.Idempotency;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.proto.artifact.PluginInfo;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Fast unit tests for {@link RemotePluginFinder}.
 */
public class RemotePluginFinderUnitTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new Gson();

  @Test
  public void testFindPluginSuccess() throws Exception {
    File rootDir = TEMP_FOLDER.newFolder();
    File artifactFile = new File(rootDir, "my-plugin-1.0.0.jar");
    Assert.assertTrue(artifactFile.createNewFile());

    String pluginsJson = GSON.toJson(Collections.singletonList(createTestPluginInfo()));

    RemoteClient remoteClient = createMockClient();
    HttpResponse pluginsResponse = createResponse(HttpURLConnection.HTTP_OK, pluginsJson);
    Mockito.when(remoteClient.execute(Mockito.any(HttpRequest.class), Mockito.eq(Idempotency.AUTO)))
        .thenReturn(pluginsResponse);

    RemoteClient remoteClientInternal = createMockClient();
    HttpResponse locationResponse =
        createResponse(HttpURLConnection.HTTP_OK, artifactFile.getAbsolutePath());
    Mockito.when(remoteClientInternal.execute(
        Mockito.any(HttpRequest.class), Mockito.eq(Idempotency.AUTO)))
        .thenReturn(locationResponse);

    RemotePluginFinder finder = new RemotePluginFinder(
        new LocalLocationFactory(rootDir), remoteClient, remoteClientInternal);
    ArtifactId parentId = NamespaceId.DEFAULT.artifact("parent", "1.0.0");

    Map.Entry<ArtifactDescriptor, PluginClass> result = finder.findPlugin(
        NamespaceId.DEFAULT, parentId, "transform", "myPlugin", new PluginSelector());
    Assert.assertEquals("my-plugin", result.getKey().getArtifactId().getName());
    Assert.assertEquals("com.example.MyPlugin", result.getValue().getClassName());
  }

  @Test
  public void testFindPluginPropagatesBadRequestIllegalArgumentUnwrapped() throws Exception {
    RemoteClient remoteClient = createMockClient();
    HttpResponse badRequestResponse =
        createResponse(HttpURLConnection.HTTP_BAD_REQUEST, "bad request");
    Mockito.when(remoteClient.execute(Mockito.any(HttpRequest.class), Mockito.eq(Idempotency.AUTO)))
        .thenReturn(badRequestResponse);
    RemoteClient remoteClientInternal = createMockClient();

    RemotePluginFinder finder = new RemotePluginFinder(
        new LocalLocationFactory(TEMP_FOLDER.newFolder()), remoteClient, remoteClientInternal);
    ArtifactId parentId = NamespaceId.DEFAULT.artifact("parent", "1.0.0");

    try {
      finder.findPlugin(NamespaceId.DEFAULT, parentId, "transform", "myPlugin", new PluginSelector());
      Assert.fail("Expected IllegalArgumentException to be propagated unwrapped");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains("400"));
    }
  }

  @Test
  public void testFindPluginPropagatesSelectorRuntimeExceptionUnwrapped() throws Exception {
    String pluginsJson = GSON.toJson(Collections.singletonList(createTestPluginInfo()));

    RemoteClient remoteClient = createMockClient();
    HttpResponse okResponse = createResponse(HttpURLConnection.HTTP_OK, pluginsJson);
    Mockito.when(remoteClient.execute(Mockito.any(HttpRequest.class), Mockito.eq(Idempotency.AUTO)))
        .thenReturn(okResponse);
    RemoteClient remoteClientInternal = createMockClient();

    RemotePluginFinder finder = new RemotePluginFinder(
        new LocalLocationFactory(TEMP_FOLDER.newFolder()), remoteClient, remoteClientInternal);
    ArtifactId parentId = NamespaceId.DEFAULT.artifact("parent", "1.0.0");
    IllegalStateException selectorFailure = new IllegalStateException("selector failed");

    PluginSelector failingSelector = new PluginSelector() {
      @Override
      public Map.Entry<io.cdap.cdap.api.artifact.ArtifactId, PluginClass> select(
          SortedMap<io.cdap.cdap.api.artifact.ArtifactId, PluginClass> plugins) {
        throw selectorFailure;
      }
    };

    try {
      finder.findPlugin(NamespaceId.DEFAULT, parentId, "transform", "myPlugin", failingSelector);
      Assert.fail("Expected selector IllegalStateException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(selectorFailure, thrown);
    }
  }

  @Test
  public void testFindPluginWrapsCheckedIoException() throws Exception {
    IOException ioException = new IOException("connection refused");
    RemoteClient remoteClient = createMockClient();
    Mockito.when(remoteClient.execute(Mockito.any(HttpRequest.class), Mockito.eq(Idempotency.AUTO)))
        .thenThrow(ioException);
    RemoteClient remoteClientInternal = createMockClient();

    RemotePluginFinder finder = new RemotePluginFinder(
        new LocalLocationFactory(TEMP_FOLDER.newFolder()), remoteClient, remoteClientInternal);
    ArtifactId parentId = NamespaceId.DEFAULT.artifact("parent", "1.0.0");

    try {
      finder.findPlugin(NamespaceId.DEFAULT, parentId, "transform", "myPlugin", new PluginSelector());
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(ioException, thrown.getCause());
    }
  }

  @Test(expected = PluginNotExistsException.class)
  public void testFindPluginConvertsArtifactNotFoundToPluginNotExists() throws Exception {
    String pluginsJson = GSON.toJson(Collections.singletonList(createTestPluginInfo()));

    RemoteClient remoteClient = createMockClient();
    HttpResponse okResponse = createResponse(HttpURLConnection.HTTP_OK, pluginsJson);
    Mockito.when(remoteClient.execute(Mockito.any(HttpRequest.class), Mockito.eq(Idempotency.AUTO)))
        .thenReturn(okResponse);

    RemoteClient remoteClientInternal = createMockClient();
    HttpResponse notFoundResponse = createResponse(HttpURLConnection.HTTP_NOT_FOUND, "missing");
    Mockito.when(remoteClientInternal.execute(
        Mockito.any(HttpRequest.class), Mockito.eq(Idempotency.AUTO)))
        .thenReturn(notFoundResponse);

    RemotePluginFinder finder = new RemotePluginFinder(
        new LocalLocationFactory(TEMP_FOLDER.newFolder()), remoteClient, remoteClientInternal);
    ArtifactId parentId = NamespaceId.DEFAULT.artifact("parent", "1.0.0");
    finder.findPlugin(NamespaceId.DEFAULT, parentId, "transform", "myPlugin", new PluginSelector());
  }

  private PluginInfo createTestPluginInfo() {
    PluginClass pluginClass = PluginClass.builder()
        .setName("myPlugin")
        .setType("transform")
        .setDescription("desc")
        .setClassName("com.example.MyPlugin")
        .setConfigFieldName("config")
        .build();
    return new PluginInfo(
        pluginClass, new ArtifactSummary("my-plugin", "1.0.0", ArtifactScope.USER));
  }

  private RemoteClient createMockClient() throws Exception {
    RemoteClient client = Mockito.mock(RemoteClient.class);
    Mockito.when(client.requestBuilder(Mockito.eq(HttpMethod.GET), Mockito.anyString()))
        .thenAnswer(invocation -> HttpRequest.get(
            new URL("http://localhost:1234/v3/" + invocation.getArgument(1))));
    return client;
  }

  private HttpResponse createResponse(int code, String body) {
    HttpResponse response = Mockito.mock(HttpResponse.class);
    Mockito.when(response.getResponseCode()).thenReturn(code);
    Mockito.when(response.getResponseBodyAsString()).thenReturn(body);
    return response;
  }
}
