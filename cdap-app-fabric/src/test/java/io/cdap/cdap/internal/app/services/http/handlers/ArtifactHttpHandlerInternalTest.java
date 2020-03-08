
package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetailFetcher;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactDetailFetcher;
import io.cdap.cdap.internal.app.runtime.artifact.app.inspection.InspectionApp;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import java.util.Set;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

public class ArtifactHttpHandlerInternalTest extends ArtifactHttpHandlerTestBase {
  @Test
  public void testGetArtifactDetail() throws Exception {
    // add a system artifact
    ArtifactId systemId = NamespaceId.SYSTEM.artifact("app", "1.0.0");
    addAppAsSystemArtifacts();

    ArtifactDetailFetcher artifactDetailFetcher = getInjector().getInstance(RemoteArtifactDetailFetcher.class);
    ArtifactDetail artifactDetail = artifactDetailFetcher.get(systemId);

//    Set<ArtifactRange> parents = Sets.newHashSet(new ArtifactRange(
//      systemId.getNamespace(), systemId.getArtifact(),
//      new ArtifactVersion(systemId.getVersion()), true, new ArtifactVersion(systemId.getVersion()), true));
//
//    Manifest manifest = new Manifest();
//    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, InspectionApp.class.getPackage().getName());
//    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("inspection", "1.0.0");
//    Assert.assertEquals(HttpResponseStatus.OK.code(),
//                        addPluginArtifact(Id.Artifact.fromEntityId(artifactId), InspectionApp.class, manifest, parents)
//                          .getResponseCode());
//    Set<PluginClass> plugins = getArtifact(artifactId).getClasses().getPlugins();
//    // Only four plugins which does not have transactions as requirement should be visible.
//    Assert.assertEquals(4, plugins.size());
//    Set<String> actualPluginClassNames = plugins.stream().map(PluginClass::getClassName).collect(Collectors.toSet());
//    Set<String> expectedPluginClassNames = ImmutableSet.of(InspectionApp.AppPlugin.class.getName(),
//                                                           InspectionApp.EmptyRequirementPlugin.class.getName(),
//                                                           InspectionApp.SingleEmptyRequirementPlugin.class.getName(),
//                                                           InspectionApp.NonTransactionalPlugin.class.getName());
//    Assert.assertEquals(expectedPluginClassNames, actualPluginClassNames);
  }
}
