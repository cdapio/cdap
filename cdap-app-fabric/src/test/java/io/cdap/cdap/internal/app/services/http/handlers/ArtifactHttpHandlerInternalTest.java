
package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.base.Predicate;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scope;
import com.google.inject.Scopes;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.ArtifactRangeNotFoundException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetailFetcher;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactDetailFetcher;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.proto.artifact.ApplicationClassInfo;
import io.cdap.cdap.proto.artifact.ApplicationClassSummary;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import javax.annotation.Nullable;

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

    cleanupSystemArtifactsDirectory();
  }

  @Test
  public void testRemoteArtifactRepositoryReader() throws Exception {
    // add a system artifact
    ArtifactId artifactId = NamespaceId.SYSTEM.artifact("app", "1.0.0");
    addAppAsSystemArtifacts();

    ArtifactRepositoryReader reader = new RemoteArtifactRepositoryReader(getDiscoveryClient(),
                                                                         getLocationFactory());
    Id.Artifact IdArtifact = new Id.Artifact(new Id.Namespace(artifactId.getNamespace()),
                                             artifactId.getArtifact(),
                                             new ArtifactVersion(artifactId.getVersion()));
    ArtifactDetail artifactDetail = reader.getArtifact(IdArtifact);

    ArtifactDescriptor descriptor = artifactDetail.getDescriptor();
    Assert.assertTrue(Artifacts.toArtifactId(NamespaceId.SYSTEM, descriptor.getArtifactId()).equals(artifactId));
    Assert.assertNotNull(descriptor.getLocation());
    Assert.assertNotNull(descriptor.getLocationURI());
    Assert.assertNotNull(artifactDetail.getMeta());

    cleanupSystemArtifactsDirectory();
  }
}
