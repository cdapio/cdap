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

import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.MockResponder;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.IOException;
import java.util.Collections;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ArtifactHttpHandler}.
 */
public class ArtifactHttpHandlerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private ArtifactHttpHandler createHandler(
      ArtifactRepository artifactRepository, NamespaceQueryAdmin namespaceQueryAdmin)
      throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    return new ArtifactHttpHandler(
        cConf,
        artifactRepository,
        namespaceQueryAdmin,
        Mockito.mock(CapabilityReader.class),
        Mockito.mock(ContextAccessEnforcer.class));
  }

  @Test
  public void testScopedNamespaceValidationAndExceptionPropagation() throws Exception {
    ArtifactRepository repo = Mockito.mock(ArtifactRepository.class);
    NamespaceQueryAdmin queryAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    NamespaceId ns1 = new NamespaceId("ns1");
    Mockito.when(queryAdmin.get(ns1))
        .thenReturn(new NamespaceMeta.Builder().setName(ns1).build());
    Mockito.when(repo.getArtifactSummaries(NamespaceId.SYSTEM, false))
        .thenReturn(Collections.emptyList());

    ArtifactHttpHandler handler = createHandler(repo, queryAdmin);
    HttpRequest request = new DefaultHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.GET, "/v3/namespaces/ns1/artifacts");

    MockResponder responder = new MockResponder();
    handler.getArtifacts(request, responder, "ns1", "system");
    Assert.assertEquals(HttpResponseStatus.OK, responder.getStatus());
    Mockito.verify(queryAdmin).get(ns1);
    Mockito.verify(repo).getArtifactSummaries(NamespaceId.SYSTEM, false);

    Assert.assertThrows(BadRequestException.class,
        () -> handler.getArtifacts(request, new MockResponder(), "ns1", "invalidScope"));

    NamespaceId missingNs = new NamespaceId("missing");
    NamespaceNotFoundException notFound = new NamespaceNotFoundException(missingNs);
    Mockito.when(queryAdmin.get(missingNs)).thenThrow(notFound);
    try {
      handler.getArtifacts(request, new MockResponder(), "missing", "user");
      Assert.fail("Expected NamespaceNotFoundException");
    } catch (NamespaceNotFoundException actual) {
      Assert.assertSame(notFound, actual);
    }

    // Unchecked RuntimeException from namespaceQueryAdmin.get must propagate unwrapped.
    NamespaceId runtimeNs = new NamespaceId("runtimeNs");
    IllegalStateException expectedRuntime = new IllegalStateException("namespace runtime error");
    Mockito.when(queryAdmin.get(runtimeNs)).thenThrow(expectedRuntime);
    try {
      handler.deleteArtifact(request, new MockResponder(), "runtimeNs", "art", "1.0.0");
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntime, actual);
    }

    // Checked Exception from namespaceQueryAdmin.get must be wrapped in RuntimeException.
    NamespaceId ioNs = new NamespaceId("ioNs");
    IOException ioException = new IOException("namespace http io error");
    Mockito.when(queryAdmin.get(ioNs)).thenThrow(ioException);
    RuntimeException wrapped = Assert.assertThrows(RuntimeException.class,
        () -> handler.deleteArtifact(request, new MockResponder(), "ioNs", "art", "1.0.0"));
    Assert.assertSame(ioException, wrapped.getCause());
  }
}
