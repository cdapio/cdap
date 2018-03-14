/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.artifact.ApplicationClass;
import co.cask.cdap.api.artifact.ArtifactClasses;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.app.runtime.DummyProgramRunnerFactory;
import co.cask.cdap.common.InvalidArtifactException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.artifact.app.InvalidConfigApp;
import co.cask.cdap.internal.app.runtime.artifact.app.inspection.InspectionApp;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.security.impersonation.DefaultImpersonator;
import co.cask.cdap.security.impersonation.EntityImpersonator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.jar.Manifest;

/**
 */
public class ArtifactInspectorTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static ArtifactClassLoaderFactory classLoaderFactory;
  private static ArtifactInspector artifactInspector;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

    classLoaderFactory = new ArtifactClassLoaderFactory(cConf, new DummyProgramRunnerFactory());
    artifactInspector = new ArtifactInspector(cConf, classLoaderFactory);
  }

  @Test(expected = InvalidArtifactException.class)
  public void testInvalidConfigApp() throws Exception {
    Manifest manifest = new Manifest();
    File appFile =
      createJar(InvalidConfigApp.class, new File(TMP_FOLDER.newFolder(), "InvalidConfigApp-1.0.0.jar"), manifest);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "InvalidConfigApp", "1.0.0");
    Location artifactLocation = Locations.toLocation(appFile);
    try (CloseableClassLoader artifactClassLoader =
           classLoaderFactory.createClassLoader(
             ImmutableList.of(artifactLocation).iterator(),
             new EntityImpersonator(artifactId.toEntityId(), new DefaultImpersonator(CConfiguration.create(), null)))) {
      artifactInspector.inspectArtifact(artifactId, appFile, artifactClassLoader);
    }
  }

  @Test
  public void inspectAppsAndPlugins() throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, InspectionApp.class.getPackage().getName());
    File appFile =
      createJar(InspectionApp.class, new File(TMP_FOLDER.newFolder(), "InspectionApp-1.0.0.jar"), manifest);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "InspectionApp", "1.0.0");
    Location artifactLocation = Locations.toLocation(appFile);
    try (CloseableClassLoader artifactClassLoader =
           classLoaderFactory.createClassLoader(
             ImmutableList.of(artifactLocation).iterator(),
             new EntityImpersonator(artifactId.toEntityId(), new DefaultImpersonator(CConfiguration.create(), null)))) {

      ArtifactClasses classes = artifactInspector.inspectArtifact(artifactId, appFile, artifactClassLoader);

      // check app classes
      Set<ApplicationClass> expectedApps = ImmutableSet.of(new ApplicationClass(
        InspectionApp.class.getName(), "", new ReflectionSchemaGenerator(false).generate(InspectionApp.AConfig.class)));
      Assert.assertEquals(expectedApps, classes.getApps());

      // check plugin classes
      PluginClass expectedPlugin = new PluginClass(
        InspectionApp.PLUGIN_TYPE, InspectionApp.PLUGIN_NAME, InspectionApp.PLUGIN_DESCRIPTION,
        InspectionApp.AppPlugin.class.getName(), "pluginConf",
        ImmutableMap.of(
          "y", new PluginPropertyField("y", "", "double", true, true),
          "isSomething", new PluginPropertyField("isSomething", "", "boolean", true, false)));
      Assert.assertEquals(ImmutableSet.of(expectedPlugin), classes.getPlugins());
    }
  }

  private static File createJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
    Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
      cls, manifest);
    DirUtils.mkdirs(destFile.getParentFile());
    Files.copy(Locations.newInputSupplier(deploymentJar), destFile);
    return destFile;
  }
}
