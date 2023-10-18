/*
 * Copyright © 2018 Cask Data, Inc.
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

import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.test.PluginJarHelper;
import io.cdap.cdap.internal.app.runtime.artifact.app.plugin.PluginTestApp;
import io.cdap.cdap.internal.app.runtime.artifact.plugin.Plugin1;
import io.cdap.cdap.common.PluginNotExistsException;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDescriptor;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.jar.Manifest;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public abstract class PluginFinderTestBase extends AppFabricTestBase {
  private static ArtifactRepository artifactRepo;

  @BeforeClass
  public static void setup() {
    artifactRepo = getInjector().getInstance(ArtifactRepository.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    ArtifactRepository artifactRepo = getInjector().getInstance(ArtifactRepository.class);
    artifactRepo.clear(NamespaceId.DEFAULT);
    artifactRepo.clear(NamespaceId.SYSTEM);
  }

  @Test
  public void testFindPlugin() throws Exception {
    // Deploy some artifacts
    File appArtifact = createAppJar(PluginTestApp.class);
    File pluginArtifact = createPluginJar(Plugin1.class);

    ArtifactId appArtifactId = NamespaceId.DEFAULT.artifact("app", "1.0");
    artifactRepo.addArtifact(Id.Artifact.fromEntityId(appArtifactId), appArtifact);

    ArtifactId pluginArtifactId = NamespaceId.DEFAULT.artifact("plugin", "1.0");
    artifactRepo.addArtifact(Id.Artifact.fromEntityId(pluginArtifactId), pluginArtifact, Collections.singleton(
      new ArtifactRange(appArtifactId.getNamespace(), appArtifactId.getArtifact(),
                        new ArtifactVersion(appArtifactId.getVersion()), true,
                        new ArtifactVersion(appArtifactId.getVersion()), true)), null);

    // Find the plugin
    PluginFinder finder = getPluginFinder();
    Map.Entry<ArtifactDescriptor, PluginClass> entry = finder.findPlugin(NamespaceId.DEFAULT, appArtifactId, "dummy",
                                                                         "Plugin1", new PluginSelector());

    Assert.assertNotNull(entry);
    Assert.assertEquals(pluginArtifactId.toApiArtifactId(), entry.getKey().getArtifactId());
    Assert.assertEquals("Plugin1", entry.getValue().getName());
    Assert.assertEquals("dummy", entry.getValue().getType());

    // Looking for a non-existing should throw
    try {
      finder.findPlugin(NamespaceId.DEFAULT, appArtifactId, "dummy2", "pluginx", new PluginSelector());
      Assert.fail("A PluginNotExistsException is expected");
    } catch (PluginNotExistsException e) {
      // expected
    }

    // Deploy the same plugin artifact to the system namespace, without any parent
    ArtifactId systemPluginArtifactId = NamespaceId.SYSTEM.artifact("pluginsystem", "1.0");
    artifactRepo.addArtifact(Id.Artifact.fromEntityId(systemPluginArtifactId), pluginArtifact);
    entry = finder.findPlugin(NamespaceId.DEFAULT, appArtifactId, "dummy", "Plugin1", new PluginSelector());
    // The selector always select the last one, hence the USER once will be selected
    Assert.assertNotNull(entry);
    Assert.assertEquals(pluginArtifactId.toApiArtifactId(), entry.getKey().getArtifactId());
    Assert.assertEquals("Plugin1", entry.getValue().getName());
    Assert.assertEquals("dummy", entry.getValue().getType());

    // Use a different selector that returns the first entry, hence expect the SYSTEM one being returned
    entry = finder.findPlugin(NamespaceId.DEFAULT, appArtifactId, "dummy", "Plugin1", new PluginSelector() {
      @Override
      public Map.Entry<io.cdap.cdap.api.artifact.ArtifactId, PluginClass> select(
        SortedMap<io.cdap.cdap.api.artifact.ArtifactId, PluginClass> plugins) {
        return plugins.entrySet().iterator().next();
      }
    });
    Assert.assertNotNull(entry);
    Assert.assertEquals(systemPluginArtifactId.toApiArtifactId(), entry.getKey().getArtifactId());
    Assert.assertEquals("Plugin1", entry.getValue().getName());
    Assert.assertEquals("dummy", entry.getValue().getType());
  }

  protected abstract PluginFinder getPluginFinder();


  private File createAppJar(Class<? extends Application> clz) throws IOException {
    LocalLocationFactory lf = new LocalLocationFactory(tmpFolder.newFolder());
    return Locations.linkOrCopy(AppJarHelper.createDeploymentJar(lf, clz),
                                new File(tmpFolder.newFolder(), clz.getSimpleName() + ".jar"));
  }

  private File createPluginJar(Class<?> clz) throws IOException {
    LocalLocationFactory lf = new LocalLocationFactory(tmpFolder.newFolder());
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, clz.getPackage().getName());
    return Locations.linkOrCopy(PluginJarHelper.createPluginJar(lf, manifest, clz),
                                new File(tmpFolder.newFolder(), clz.getSimpleName() + ".jar"));
  }
}
