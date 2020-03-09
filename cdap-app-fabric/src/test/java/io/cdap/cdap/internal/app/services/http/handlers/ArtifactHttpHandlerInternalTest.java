
package io.cdap.cdap.internal.app.services.http.handlers;

import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetailFetcher;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactDetailFetcher;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;

public class ArtifactHttpHandlerInternalTest extends ArtifactHttpHandlerTestBase {
  @Test
  public void testGetArtifactDetail() throws Exception {
    // add a system artifact
    ArtifactId artifactId = NamespaceId.SYSTEM.artifact("app", "1.0.0");
    addAppAsSystemArtifacts();

    ArtifactDetailFetcher artifactDetailFetcher = getInjector().getInstance(RemoteArtifactDetailFetcher.class);
    ArtifactDetail artifactDetail = artifactDetailFetcher.get(artifactId);

    Assert.assertTrue(artifactId.toApiArtifactId().equals(artifactDetail.getDescriptor().getArtifactId()));
    Assert.assertNull(artifactDetail.getDescriptor().getLocation());
    Assert.assertNotNull(artifactDetail.getDescriptor().getLocationURI());
    Assert.assertNotNull(artifactDetail.getMeta().getClasses());
    Assert.assertNotNull(artifactDetail.getMeta().getProperties());
  }
}
