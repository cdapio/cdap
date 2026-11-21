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

package io.cdap.cdap.gateway.handlers;

import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactMeta;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Fast unit tests for {@link ArtifactHttpHandlerInternal} namespace validation and exception propagation.
 */
public class ArtifactHttpHandlerInternalUnitTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testGetArtifactBytesNamespaceValidationAndExceptions() throws Exception {
    ArtifactRepository repo = Mockito.mock(ArtifactRepository.class);
    final NamespaceQueryAdmin queryAdmin = Mockito.mock(NamespaceQueryAdmin.class);

    File jarFile = TEMP_FOLDER.newFile("artifact-1.0.0.jar");
    Files.write(jarFile.toPath(), "jar-bytes".getBytes(StandardCharsets.UTF_8));
    ArtifactDetail detail = new ArtifactDetail(
        new ArtifactDescriptor(
            NamespaceId.SYSTEM.getNamespace(),
            new ArtifactId("art", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
            Locations.toLocation(jarFile)),
        new ArtifactMeta(ArtifactClasses.builder().build()));
    Mockito.when(repo.getArtifact(Id.Artifact.from(Id.Namespace.SYSTEM, "art", "1.0.0")))
        .thenReturn(detail);
    Mockito.when(repo.getArtifact(Id.Artifact.from(Id.Namespace.from("ns1"), "art", "1.0.0")))
        .thenReturn(detail);

    ArtifactHttpHandlerInternal handler = new ArtifactHttpHandlerInternal(repo, queryAdmin);
    HttpRequest request = new DefaultHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.GET, "/v3Internal/namespaces/ns1/artifacts/art/versions/1.0.0/download");
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    // System scope skips namespaceQueryAdmin.get for user namespace.
    handler.getArtifactBytes(request, responder, "ns1", "art", "1.0.0", "system");
    Mockito.verify(queryAdmin, Mockito.never()).get(Mockito.any());
    Mockito.verify(responder).sendContent(
        Mockito.eq(HttpResponseStatus.OK), Mockito.any(BodyProducer.class),
        Mockito.any(HttpHeaders.class));

    // User scope validates namespace.
    NamespaceId ns1 = new NamespaceId("ns1");
    Mockito.when(queryAdmin.get(ns1)).thenReturn(new NamespaceMeta.Builder().setName(ns1).build());
    handler.getArtifactBytes(request, responder, "ns1", "art", "1.0.0", "user");
    Mockito.verify(queryAdmin).get(ns1);

    Assert.assertThrows(BadRequestException.class,
        () -> handler.getArtifactBytes(request, responder, "ns1", "art", "1.0.0", "badScope"));

    NamespaceId missingNs = new NamespaceId("missing");
    NamespaceNotFoundException notFound = new NamespaceNotFoundException(missingNs);
    Mockito.when(queryAdmin.get(missingNs)).thenThrow(notFound);
    try {
      handler.getArtifactBytes(request, responder, "missing", "art", "1.0.0", "user");
      Assert.fail("Expected NamespaceNotFoundException");
    } catch (NamespaceNotFoundException actual) {
      Assert.assertSame(notFound, actual);
    }

    // Unchecked RuntimeException propagates unwrapped.
    NamespaceId runtimeNs = new NamespaceId("runtimeNs");
    IllegalStateException expectedRuntime = new IllegalStateException("admin runtime failure");
    Mockito.when(queryAdmin.get(runtimeNs)).thenThrow(expectedRuntime);
    try {
      handler.getArtifactBytes(request, responder, "runtimeNs", "art", "1.0.0", "user");
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntime, actual);
    }

    // Checked Exception is wrapped in RuntimeException.
    NamespaceId ioNs = new NamespaceId("ioNs");
    IOException ioException = new IOException("admin io failure");
    Mockito.when(queryAdmin.get(ioNs)).thenThrow(ioException);
    RuntimeException wrapped = Assert.assertThrows(RuntimeException.class,
        () -> handler.getArtifactBytes(request, responder, "ioNs", "art", "1.0.0", "user"));
    Assert.assertSame(ioException, wrapped.getCause());
  }
}
