/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.capability.autoinstall;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.proto.artifact.ArtifactRanges;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.File;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link HubPackage}.
 */
public class HubPackageTest {

  private static final Gson GSON = new Gson();

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testInstallPluginStreamsJarClosesChannelAndCleansUpTempFile() throws Exception {
    byte[] jarBytes = "fake-plugin-jar-binary-payload-0123456789".getBytes(StandardCharsets.UTF_8);
    List<Spec.Action.Argument> args = Arrays.asList(
        new Spec.Action.Argument("config", "my-plugin-1.0.0.json", false),
        new Spec.Action.Argument("jar", "my-plugin-1.0.0.jar", false));
    Spec.Action deployAction = new Spec.Action("one_step_deploy_plugin", "Deploy", args);
    Spec.Action ignoredAction = new Spec.Action("other_action", "Ignore", Collections.emptyList());
    Spec spec = new Spec(
        "1.0", "My Plugin", "Desc", "Author", "Org", 100L, "[6.0.0,7.0.0)",
        Collections.singletonList("cat"), false, Arrays.asList(ignoredAction, deployAction));

    Map<String, String> pluginProps = ImmutableMap.of("widgets.MyPlugin", "{\"k\":\"v\"}");
    Map<String, Object> configPayload = ImmutableMap.of(
        "parents", ImmutableList.of("system:cdap-data-pipeline[6.0.0,7.0.0)", "cdap-data-streams[6.0.0,7.0.0)"),
        "properties", pluginProps);

    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/hub/packages/my-plugin/1.0.0/spec.json", exchange -> {
      byte[] body = GSON.toJson(spec).getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    });
    server.createContext("/hub/packages/my-plugin/1.0.0/my-plugin-1.0.0.json", exchange -> {
      byte[] body = GSON.toJson(configPayload).getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    });
    server.createContext("/hub/packages/my-plugin/1.0.0/my-plugin-1.0.0.jar", exchange -> {
      exchange.sendResponseHeaders(200, jarBytes.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(jarBytes);
      }
    });
    server.start();

    try {
      URL baseUrl = new URL(
          "http", server.getAddress().getHostString(), server.getAddress().getPort(), "/hub");
      HubPackage hubPackage = new HubPackage(
          "my-plugin", "1.0.0", "Desc", "My Plugin", "Author", "Org",
          "[6.0.0,7.0.0)", 100L, false, Collections.singletonList("cat"), false, null);

      ArtifactRepository artifactRepository = Mockito.mock(ArtifactRepository.class);
      AtomicReference<byte[]> capturedJarBytes = new AtomicReference<>();
      AtomicReference<File> capturedFile = new AtomicReference<>();
      Mockito.doAnswer(invocation -> {
        File downloaded = invocation.getArgument(1);
        capturedFile.set(downloaded);
        capturedJarBytes.set(Files.readAllBytes(downloaded.toPath()));
        throw new ArtifactAlreadyExistsException(NamespaceId.DEFAULT.artifact("my-plugin", "1.0.0"));
      }).when(artifactRepository).addArtifact(
          Mockito.any(), Mockito.any(File.class), Mockito.anySet(), Mockito.anySet());

      File tmpDir = TEMP_FOLDER.newFolder();
      hubPackage.installPlugin(baseUrl, artifactRepository, tmpDir);

      Assert.assertArrayEquals(jarBytes, capturedJarBytes.get());
      Assert.assertNotNull(capturedFile.get());
      Assert.assertFalse("Downloaded temp file must be deleted after install", capturedFile.get().exists());

      Id.Artifact expectedArtifact = Id.Artifact.from(Id.Namespace.DEFAULT, "my-plugin", "1.0.0");
      Set<ArtifactRange> expectedParents = ImmutableSet.of(
          ArtifactRanges.parseArtifactRange("system:cdap-data-pipeline[6.0.0,7.0.0)"),
          ArtifactRanges.parseArtifactRange("default", "cdap-data-streams[6.0.0,7.0.0)"));
      Mockito.verify(artifactRepository).addArtifact(
          Mockito.eq(expectedArtifact), Mockito.any(File.class),
          Mockito.eq(expectedParents), Mockito.eq(ImmutableSet.of()));
      Mockito.verify(artifactRepository).writeArtifactProperties(expectedArtifact, pluginProps);
    } finally {
      server.stop(0);
    }
  }
}
