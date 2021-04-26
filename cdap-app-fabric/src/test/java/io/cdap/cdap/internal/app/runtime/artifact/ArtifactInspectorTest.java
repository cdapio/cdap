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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.app.runtime.DummyProgramRunnerFactory;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.artifact.app.InvalidConfigApp;
import io.cdap.cdap.internal.app.runtime.artifact.app.inspection.InspectionApp;
import io.cdap.cdap.internal.app.runtime.artifact.plugin.nested.NestedConfigPlugin;
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
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
  @Ignore
  // TODO: (CDAP-16919) Re-enable this test once schema mapping for Java Object is fixed.
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
      artifactInspector.inspectArtifact(artifactId, appFile, artifactClassLoader, Collections.emptySet());
    }
  }

  @Test
  public void testGetPluginRequirements() {
    // check that if a plugin does not specify requirement annotation then it has empty requirements
    Assert.assertTrue(artifactInspector.getArtifactRequirements(InspectionApp.AppPlugin.class).isEmpty());

    // check that if a plugin specify a requirement it is captured
    Assert.assertEquals(new Requirements(ImmutableSet.of(Table.TYPE)),
                        artifactInspector.getArtifactRequirements(InspectionApp.SingleRequirementPlugin.class));

    // check if a plugin specify a requirement annotation but it is empty the requirement captured is no requirement
    Assert
      .assertTrue(artifactInspector.getArtifactRequirements(InspectionApp.EmptyRequirementPlugin.class).isEmpty());

    // check if a plugin specify multiple requirement all of them are captured
    Assert.assertEquals(new Requirements(ImmutableSet.of(Table.TYPE, KeyValueTable.TYPE)),
                        artifactInspector.getArtifactRequirements(InspectionApp.MultipleRequirementsPlugin.class));

    // check if a plugin has specified empty string as requirement is captured as no requirements
    Assert.assertTrue(artifactInspector
                        .getArtifactRequirements(InspectionApp.SingleEmptyRequirementPlugin.class).isEmpty());

    // check if a plugin has specified empty string with a valid requirement only the valid requirement is captured
    Assert.assertEquals(new Requirements(ImmutableSet.of(Table.TYPE)),
                        artifactInspector
                          .getArtifactRequirements(InspectionApp.ValidAndEmptyRequirementsPlugin.class));

    // test that duplicate requirements are only stored once and the beginning and ending white spaces are trimmed
    Assert.assertEquals(new Requirements(ImmutableSet.of(Table.TYPE, "duplicate")),
                        artifactInspector.getArtifactRequirements(InspectionApp.DuplicateRequirementsPlugin.class));

    //Test that capabilities in the Requirements annotation is being captured
    Assert.assertEquals(new Requirements(ImmutableSet.of(), ImmutableSet.of("cdc")),
                        artifactInspector.getArtifactRequirements(InspectionApp.CapabilityPlugin.class));
    Assert.assertEquals(new Requirements(ImmutableSet.of(), ImmutableSet.of("cdc", "healthcare")),
                        artifactInspector.getArtifactRequirements(InspectionApp.MultipleCapabilityPlugin.class));
    Assert.assertEquals(new Requirements(ImmutableSet.of(Table.TYPE, "sometype"), ImmutableSet.of("cdc", "healthcare")),
                        artifactInspector.getArtifactRequirements(InspectionApp.DatasetAndCapabilityPlugin.class));
  }

  @Test
  public void inspectAppsAndPlugins() throws Exception {
    File appFile = getAppFile();
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "InspectionApp", "1.0.0");
    Location artifactLocation = Locations.toLocation(appFile);
    try (CloseableClassLoader artifactClassLoader =
           classLoaderFactory.createClassLoader(
             ImmutableList.of(artifactLocation).iterator(),
             new EntityImpersonator(artifactId.toEntityId(), new DefaultImpersonator(CConfiguration.create(), null)))) {

      ArtifactClasses classes = artifactInspector.inspectArtifact(artifactId, appFile, artifactClassLoader,
                                                                  Collections.emptySet());

      // check app classes
      Set<ApplicationClass> expectedApps = ImmutableSet.of(new ApplicationClass(
        InspectionApp.class.getName(), "", new ReflectionSchemaGenerator(false).generate(InspectionApp.AConfig.class),
        new Requirements(Collections.emptySet(), Collections.singleton("cdc"))));
      Assert.assertEquals(expectedApps, classes.getApps());

      // check plugin classes
      PluginClass expectedPlugin =
        PluginClass.builder().setName(InspectionApp.PLUGIN_NAME).setType(InspectionApp.PLUGIN_TYPE)
          .setDescription(InspectionApp.PLUGIN_DESCRIPTION).setClassName(InspectionApp.AppPlugin.class.getName())
          .setConfigFieldName("pluginConf").setProperties(ImmutableMap.of(
          "y", new PluginPropertyField("y", "", "double", true, true),
          "isSomething", new PluginPropertyField("isSomething", "", "boolean", true, false))).build();

      PluginClass multipleRequirementPlugin = PluginClass.builder()
        .setName(InspectionApp.MULTIPLE_REQUIREMENTS_PLUGIN)
        .setType(InspectionApp.PLUGIN_TYPE)
        .setCategory(InspectionApp.PLUGIN_CATEGORY)
        .setClassName(InspectionApp.MultipleRequirementsPlugin.class.getName())
        .setConfigFieldName("pluginConf")
        .setProperties(ImmutableMap.of(
          "y", new PluginPropertyField("y", "", "double", true, true),
          "isSomething", new PluginPropertyField("isSomething", "", "boolean", true, false)))
        .setRequirements(new Requirements(ImmutableSet.of(Table.TYPE, KeyValueTable.TYPE)))
        .setDescription(InspectionApp.PLUGIN_DESCRIPTION)
        .build();
      Assert.assertTrue(classes.getPlugins().containsAll(ImmutableSet.of(expectedPlugin, multipleRequirementPlugin)));
    }
  }

  @Test
  public void testInspectNestedConfigPlugin() throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, NestedConfigPlugin.class.getPackage().getName());
    File artifactFile = createJar(NestedConfigPlugin.class, new File(TMP_FOLDER.newFolder(), "NestedPlugin-1.0.0.jar"),
                                  manifest);
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "NestedPlugin", "1.0.0");
    Location artifactLocation = Locations.toLocation(artifactFile);
    try (CloseableClassLoader artifactClassLoader =
           classLoaderFactory.createClassLoader(
             ImmutableList.of(artifactLocation).iterator(),
             new EntityImpersonator(artifactId.toEntityId(), new DefaultImpersonator(CConfiguration.create(), null)))) {
      ArtifactClasses classes = artifactInspector.inspectArtifact(artifactId, artifactFile, artifactClassLoader,
                                                                  Collections.emptySet());
      Set<PluginClass> plugins = classes.getPlugins();

      Map<String, PluginPropertyField> expectedFields = ImmutableMap.of(
        "X", new PluginPropertyField("X", "", "int", true, false),
        "Nested", new PluginPropertyField("Nested", "", "nestedconfig", true, true, false,
                                          ImmutableSet.of("Nested1", "Nested2")),
        "Nested1", new PluginPropertyField("Nested1", "", "string", true, true),
        "Nested2", new PluginPropertyField("Nested2", "", "string", true, true)
      );

      PluginClass expected = PluginClass.builder()
                               .setName("nested").setType("dummy").setDescription("Nested config")
                               .setClassName(NestedConfigPlugin.class.getName()).setConfigFieldName("config")
                               .setProperties(expectedFields).build();

      Assert.assertEquals(Collections.singleton(expected), plugins);
    }
  }

  @Test(expected = InvalidArtifactException.class)
  public void inspectAdditionaPluginClasses() throws Exception {
    File artifactFile = createJar(InspectionApp.class, new File(TMP_FOLDER.newFolder(), "InspectionApp-1.0.0.jar"),
                                  new Manifest());
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "InspectionApp", "1.0.0");
    Location artifactLocation = Locations.toLocation(artifactFile);
    try (CloseableClassLoader artifactClassLoader =
           classLoaderFactory.createClassLoader(
             ImmutableList.of(artifactLocation).iterator(),
             new EntityImpersonator(artifactId.toEntityId(), new DefaultImpersonator(CConfiguration.create(), null)))) {

      // PluginClass contains a non existing classname that is not present in the artifact jar being used
      PluginClass pluginClass =
        PluginClass.builder().setName("plugin_name").setType("plugin_type")
          .setDescription("").setClassName("non-existing-class")
          .setConfigFieldName("pluginConf").setProperties(ImmutableMap.of()).build();
      // Inspects the jar and ensures that additional plugin classes can be loaded from the artifact jar
      artifactInspector.inspectArtifact(artifactId, artifactFile, artifactClassLoader, ImmutableSet.of(pluginClass));
    }
  }

  @Test
  public void testGetClassNameCheckPredicate() {
    Assert.assertTrue(ArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "my.package"
    )).test("my/package/my.class"));
    Assert.assertTrue(ArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "blah", "my.package", "blah2"
    )).test("my/package/my.class"));
    Assert.assertTrue(ArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "my.package", "my.package.subpackage"
    )).test("my/package/subpackage/my.class"));
    Assert.assertFalse(ArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "my.package"
    )).test("my/package/subpackage/my.class"));
    Assert.assertFalse(ArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "my.package"
    )).test("prefix/my/package/my.class"));
    Assert.assertFalse(ArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "my.package"
    )).test("prefix/my/package/my.notclass"));
    Assert.assertFalse(ArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "my.package"
    )).test("prefix/my/package/my.class/some.class"));
  }

  private File getAppFile() throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, InspectionApp.class.getPackage().getName());
    return createJar(InspectionApp.class, new File(TMP_FOLDER.newFolder(), "InspectionApp-1.0.0.jar"), manifest);
  }

  private static File createJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
    Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
      cls, manifest);
    DirUtils.mkdirs(destFile.getParentFile());
    Locations.linkOrCopyOverwrite(deploymentJar, destFile);
    return destFile;
  }
}
