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

package io.cdap.cdap.internal.app.runtime.artifact;

import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ArtifactExistenceVerifier}.
 */
public class ArtifactExistenceVerifierTest {

  @Test
  public void testEnsureExistsAndExceptionPropagation() throws Exception {
    ArtifactStore store = Mockito.mock(ArtifactStore.class);
    ArtifactExistenceVerifier verifier = new ArtifactExistenceVerifier(store);

    ArtifactId existing = NamespaceId.DEFAULT.artifact("existing", "1.0.0");
    Mockito.when(store.getArtifact(Id.Artifact.fromEntityId(existing)))
        .thenReturn(Mockito.mock(ArtifactDetail.class));
    verifier.ensureExists(existing);

    ArtifactId missing = NamespaceId.DEFAULT.artifact("missing", "1.0.0");
    ArtifactNotFoundException expectedNotFound = new ArtifactNotFoundException(missing);
    Mockito.when(store.getArtifact(Id.Artifact.fromEntityId(missing))).thenThrow(expectedNotFound);
    try {
      verifier.ensureExists(missing);
      Assert.fail("Expected ArtifactNotFoundException");
    } catch (ArtifactNotFoundException actual) {
      Assert.assertSame(expectedNotFound, actual);
    }

    ArtifactId ioArtifact = NamespaceId.DEFAULT.artifact("ioArt", "1.0.0");
    IOException ioException = new IOException("table read error");
    Mockito.when(store.getArtifact(Id.Artifact.fromEntityId(ioArtifact))).thenThrow(ioException);
    RuntimeException wrappedIo = Assert.assertThrows(
        RuntimeException.class, () -> verifier.ensureExists(ioArtifact));
    Assert.assertSame(ioException, wrappedIo.getCause());

    ArtifactId runtimeArtifact = NamespaceId.DEFAULT.artifact("rtArt", "1.0.0");
    IllegalStateException expectedRuntime = new IllegalStateException("unexpected state");
    Mockito.when(store.getArtifact(Id.Artifact.fromEntityId(runtimeArtifact)))
        .thenThrow(expectedRuntime);
    try {
      verifier.ensureExists(runtimeArtifact);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntime, actual);
    }
  }
}
