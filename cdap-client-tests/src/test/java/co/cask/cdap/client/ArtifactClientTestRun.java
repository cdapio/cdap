/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.client.artifact.MyApp;
import co.cask.cdap.client.artifact.plugin.Plugin1;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.common.test.PluginJarHelper;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ApplicationClass;
import co.cask.cdap.proto.artifact.ApplicationClassInfo;
import co.cask.cdap.proto.artifact.ApplicationClassSummary;
import co.cask.cdap.proto.artifact.ArtifactClasses;
import co.cask.cdap.proto.artifact.ArtifactInfo;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.artifact.PluginInfo;
import co.cask.cdap.proto.artifact.PluginSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.InputSupplier;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.jar.Manifest;

/**
 * Test for {@link ArtifactClient}
 */
@Category(XSlowTests.class)
public class ArtifactClientTestRun extends ClientTestBase {
  private static final InputSupplier<InputStream> DUMMY_SUPPLIER = new InputSupplier<InputStream>() {
    @Override
    public InputStream getInput() throws IOException {
      return new ByteArrayInputStream(new byte[]{});
    }
  };

  private ArtifactClient artifactClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    artifactClient = new ArtifactClient(clientConfig, new RESTClient(clientConfig));
    for (ArtifactSummary artifactSummary : artifactClient.list(NamespaceId.DEFAULT)) {
      artifactClient.delete(
        NamespaceId.DEFAULT.artifact(artifactSummary.getName(), artifactSummary.getVersion()));
    }
  }

  @Test
  public void testNotFound() throws Exception {
    ArtifactId ghostId = NamespaceId.DEFAULT.artifact("ghost", "1.0.0");
    try {
      artifactClient.list(new NamespaceId("ghost"));
      Assert.fail();
    } catch (NotFoundException e) {
      // expected
    }

    try {
      artifactClient.getArtifactInfo(ghostId);
      Assert.fail();
    } catch (ArtifactNotFoundException e) {
      // expected
    }

    try {
      artifactClient.listVersions(new NamespaceId(ghostId.getNamespace()), ghostId.getEntityName());
      Assert.fail();
    } catch (ArtifactNotFoundException e) {
      // expected
    }

    // test adding an artifact that extends a non-existent artifact
    Set<ArtifactRange> parents = Sets.newHashSet(
      new ArtifactRange(ghostId.getParent(), ghostId.getArtifact(),
                        new ArtifactVersion("1"), new ArtifactVersion("2")));
    try {
      artifactClient.add(NamespaceId.DEFAULT, "abc", DUMMY_SUPPLIER, "1.0.0", parents);
      Assert.fail();
    } catch (NotFoundException e) {
      // expected
    }

    try {
      artifactClient.getPluginTypes(ghostId);
      Assert.fail();
    } catch (ArtifactNotFoundException e) {
      // expected
    }

    try {
      artifactClient.getPluginSummaries(ghostId, "type");
      Assert.fail();
    } catch (ArtifactNotFoundException e) {
      // expected
    }

    try {
      artifactClient.getPluginInfo(ghostId, "type", "name");
      Assert.fail();
    } catch (NotFoundException e) {
      // expected
    }
  }

  @Test
  public void testAddArtifactBadIds() throws Exception {
    // test bad version
    try {
      artifactClient.add(NamespaceId.DEFAULT, "abc", DUMMY_SUPPLIER, "1/0.0");
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    // test bad name
    try {
      artifactClient.add(NamespaceId.DEFAULT, "ab:c", DUMMY_SUPPLIER, "1.0.0");
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }
  }

  @Test
  public void testAddSelfExtendingThrowsBadRequest() throws Exception {
    try {
      artifactClient.add(NamespaceId.DEFAULT, "abc", DUMMY_SUPPLIER, "1.0.0", Sets.newHashSet(
        new ArtifactRange(NamespaceId.DEFAULT, "abc", new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0"))
      ));
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }
  }

  @Test
  public void testArtifacts() throws Exception {
    // add 2 versions of an artifact with an application
    ArtifactId myapp1Id = NamespaceId.DEFAULT.artifact("myapp", "1.0.0");
    ArtifactId myapp2Id = NamespaceId.DEFAULT.artifact("myapp", "2.0.0");
    LocalLocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.BUNDLE_VERSION, "2.0.0");
    final Location appJarLoc = AppJarHelper.createDeploymentJar(locationFactory, MyApp.class, manifest);

    InputSupplier<InputStream> inputSupplier = new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return appJarLoc.getInputStream();
      }
    };
    artifactClient.add(new NamespaceId(myapp1Id.getNamespace()), myapp1Id.getEntityName(),
                       inputSupplier, myapp1Id.getVersion());
    // add some properties
    Map<String, String> myapp1Properties = ImmutableMap.of("k1", "v1");
    artifactClient.writeProperties(myapp1Id, myapp1Properties);

    // let it derive version from jar manifest, which has bundle-version at 2.0.0
    artifactClient.add(new NamespaceId(myapp2Id.getNamespace()), myapp2Id.getEntityName(), inputSupplier, null, null);
    // add some properties
    Map<String, String> myapp2Properties = ImmutableMap.of("k1", "v1", "k2", "v2");
    artifactClient.writeProperties(myapp2Id, myapp2Properties);

    // add an artifact that contains a plugin, but only extends myapp-2.0.0
    ArtifactId pluginId = NamespaceId.DEFAULT.artifact("myapp-plugins", "2.0.0");
    manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, Plugin1.class.getPackage().getName());
    final Location pluginJarLoc = PluginJarHelper.createPluginJar(locationFactory, manifest, Plugin1.class);
    inputSupplier = new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return pluginJarLoc.getInputStream();
      }
    };
    Set<ArtifactRange> parents = Sets.newHashSet(new ArtifactRange(
      myapp2Id.getParent(), myapp2Id.getArtifact(),
      new ArtifactVersion(myapp2Id.getVersion()), new ArtifactVersion("3.0.0")));
    Set<PluginClass> additionalPlugins = Sets.newHashSet(new PluginClass(
      "jdbc", "mysql", "", "com.mysql.jdbc.Driver", null, Collections.<String, PluginPropertyField>emptyMap()));
    artifactClient.add(new NamespaceId(pluginId.getNamespace()), pluginId.getEntityName(), inputSupplier,
                       pluginId.getVersion(), parents, additionalPlugins);

    ArtifactSummary myapp1Summary = new ArtifactSummary(myapp1Id.getEntityName(), myapp1Id.getVersion());
    ArtifactSummary myapp2Summary = new ArtifactSummary(myapp2Id.getEntityName(), myapp2Id.getVersion());
    ArtifactSummary pluginArtifactSummary = new ArtifactSummary(pluginId.getEntityName(), pluginId.getVersion());

    Set<ArtifactSummary> artifacts = Sets.newHashSet(artifactClient.list(NamespaceId.DEFAULT));
    Assert.assertEquals(Sets.newHashSet(myapp1Summary, myapp2Summary, pluginArtifactSummary), artifacts);

    // list all artifacts named 'myapp'
    Assert.assertEquals(Sets.newHashSet(myapp1Summary, myapp2Summary),
                        Sets.newHashSet(artifactClient.listVersions(NamespaceId.DEFAULT, myapp1Id.getEntityName())));
    // list all artifacts named 'myapp-plugins'
    Assert.assertEquals(Sets.newHashSet(pluginArtifactSummary),
                        Sets.newHashSet(artifactClient.listVersions(NamespaceId.DEFAULT, pluginId.getEntityName())));
    // artifacts should be in user scope
    try {
      artifactClient.listVersions(NamespaceId.DEFAULT, pluginId.getEntityName(), ArtifactScope.SYSTEM);
      Assert.fail();
    } catch (ArtifactNotFoundException e) {
      // expected
    }

    // get info about specific artifacts
    Schema myAppConfigSchema = new ReflectionSchemaGenerator(false).generate(MyApp.Conf.class);
    ArtifactClasses myAppClasses = ArtifactClasses.builder()
      .addApp(new ApplicationClass(MyApp.class.getName(), "", myAppConfigSchema))
      .build();

    // test get myapp-1.0.0
    ArtifactInfo myapp1Info =
      new ArtifactInfo(myapp1Id.getEntityName(), myapp1Id.getVersion(), ArtifactScope.USER,
                       myAppClasses, myapp1Properties);
    Assert.assertEquals(myapp1Info, artifactClient.getArtifactInfo(myapp1Id));

    // test get myapp-2.0.0
    ArtifactInfo myapp2Info =
      new ArtifactInfo(myapp2Id.getEntityName(), myapp2Id.getVersion(), ArtifactScope.USER,
                       myAppClasses, myapp2Properties);
    Assert.assertEquals(myapp2Info, artifactClient.getArtifactInfo(myapp2Id));
    // test overwriting properties
    myapp2Properties = ImmutableMap.of("k1", "v3", "k5", "v5");
    artifactClient.writeProperties(myapp2Id, myapp2Properties);
    Assert.assertEquals(myapp2Properties, artifactClient.getArtifactInfo(myapp2Id).getProperties());
    // test deleting property
    artifactClient.deleteProperty(myapp2Id, "k1");
    Assert.assertEquals(ImmutableMap.of("k5", "v5"),
                        artifactClient.getArtifactInfo(myapp2Id).getProperties());
    // test writing property
    artifactClient.writeProperty(myapp2Id, "k5", "v4");
    Assert.assertEquals(ImmutableMap.of("k5", "v4"),
                        artifactClient.getArtifactInfo(myapp2Id).getProperties());
    // test deleting properties
    artifactClient.deleteProperties(myapp2Id);
    Assert.assertTrue(artifactClient.getArtifactInfo(myapp2Id).getProperties().isEmpty());


    // test get myapp-plugins-2.0.0
    Map<String, PluginPropertyField> props = ImmutableMap.of(
      "x", new PluginPropertyField("x", "", "int", true, false));
    ArtifactClasses pluginClasses = ArtifactClasses.builder()
      .addPlugin(new PluginClass("callable", "plugin1", "p1 description", Plugin1.class.getName(), "conf", props))
      .addPlugins(additionalPlugins)
      .build();
    ArtifactInfo pluginArtifactInfo =
      new ArtifactInfo(pluginId.getEntityName(), pluginId.getVersion(), ArtifactScope.USER,
                       pluginClasses, ImmutableMap.<String, String>of());
    Assert.assertEquals(pluginArtifactInfo, artifactClient.getArtifactInfo(pluginId));

    // test get all app classes in namespace
    Set<ApplicationClassSummary> expectedSummaries = ImmutableSet.of(
      new ApplicationClassSummary(myapp1Summary, MyApp.class.getName()),
      new ApplicationClassSummary(myapp2Summary, MyApp.class.getName())
    );
    Set<ApplicationClassSummary> appClassSummaries = Sets.newHashSet(
      artifactClient.getApplicationClasses(Id.Namespace.DEFAULT));
    Assert.assertEquals(expectedSummaries, appClassSummaries);

    // test get all app classes in namespace with name MyApp.class.getName()
    Set<ApplicationClassInfo> appClassInfos = Sets.newHashSet(
      artifactClient.getApplicationClasses(Id.Namespace.DEFAULT, MyApp.class.getName()));
    Set<ApplicationClassInfo> expectedInfos = ImmutableSet.of(
      new ApplicationClassInfo(myapp1Summary, MyApp.class.getName(), myAppConfigSchema),
      new ApplicationClassInfo(myapp2Summary, MyApp.class.getName(), myAppConfigSchema)
    );
    Assert.assertEquals(expectedInfos, appClassInfos);

    // test get plugin types for myapp-1.0.0. should be empty, since plugins only extends versions [2.0.0 - 3.0.0)
    Assert.assertTrue(artifactClient.getPluginTypes(myapp1Id).isEmpty());

    // test get plugin types for myapp-2.0.0
    Assert.assertEquals(Lists.newArrayList("callable", "jdbc"), artifactClient.getPluginTypes(myapp2Id));

    // test get plugins of type callable for myapp-2.0.0
    PluginSummary pluginSummary =
      new PluginSummary("plugin1", "callable", "p1 description", Plugin1.class.getName(), pluginArtifactSummary);
    Assert.assertEquals(Sets.newHashSet(pluginSummary),
                        Sets.newHashSet(artifactClient.getPluginSummaries(myapp2Id, "callable")));
    // no plugins of type "runnable"
    Assert.assertTrue(artifactClient.getPluginSummaries(myapp2Id, "runnable").isEmpty());

    // test get plugin details for plugin1 for myapp-2.0.0
    PluginInfo pluginInfo = new PluginInfo("plugin1", "callable", "p1 description", Plugin1.class.getName(),
      pluginArtifactSummary, props, new HashSet<String>());
    Assert.assertEquals(Sets.newHashSet(pluginInfo),
                        Sets.newHashSet(artifactClient.getPluginInfo(myapp2Id, "callable", "plugin1")));
  }

}
