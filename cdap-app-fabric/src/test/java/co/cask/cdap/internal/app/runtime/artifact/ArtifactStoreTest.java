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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.runtime.artifact.app.InspectionApp;
import co.cask.cdap.internal.artifact.ArtifactVersion;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.SlowTests;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 */
// suppressing warnings for Bytes.toBytes() when we know the result is not null
@SuppressWarnings("ConstantConditions")
public class ArtifactStoreTest {
  private static ArtifactStore artifactStore;

  @BeforeClass
  public static void setup() throws Exception {
    artifactStore = AppFabricTestHelper.getInjector().getInstance(ArtifactStore.class);
  }

  @After
  public void cleanup() throws IOException {
    artifactStore.clear(Constants.DEFAULT_NAMESPACE_ID);
  }

  @Test
  public void testGetNonexistantArtifact() throws IOException {
    Id.Namespace namespace = Id.Namespace.from("ns1");

    // no artifacts in a namespace should return an empty collection
    Assert.assertTrue(artifactStore.getArtifacts(namespace).isEmpty());
    // no artifacts in range should return an empty collection
    ArtifactRange range = new ArtifactRange(
      namespace, "something", new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0"));
    Assert.assertTrue(artifactStore.getArtifacts(range).isEmpty());

    // no artifact by namespace and artifact name should throw an exception
    try {
      artifactStore.getArtifacts(namespace, "something");
      Assert.fail();
    } catch (ArtifactNotExistsException e) {
      // expected
    }

    // no artifact by namespace, artifact name, and version should throw an exception
    try {
      artifactStore.getArtifact(Id.Artifact.from(namespace, "something", "1.0.0"));
      Assert.fail();
    } catch (ArtifactNotExistsException e) {
      // expected
    }
  }

  @Test
  public void testGetNonexistantPlugin() throws IOException {
    Id.Artifact parentArtifact = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "abc", "1.2.3");

    // no plugins in a namespace should return an empty collection
    Assert.assertTrue(artifactStore.getPluginClasses(parentArtifact).isEmpty());

    // no plugins in namespace of a given type should return an empty map
    Assert.assertTrue(artifactStore.getPluginClasses(parentArtifact, "sometype").isEmpty());

    // no plugins in namespace of a given type and name should throw an exception
    try {
      artifactStore.getPluginClasses(parentArtifact, "sometype", "somename");
      Assert.fail();
    } catch (PluginNotExistsException e) {
      // expected
    }
  }

  @Test
  public void testAddGetSingleArtifact() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "myplugins", "1.0.0");
    List<PluginClass> plugins = ImmutableList.of(
      new PluginClass("atype", "plugin1", "", "c.c.c.plugin1", "cfg", ImmutableMap.<String, PluginPropertyField>of()),
      new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg", ImmutableMap.<String, PluginPropertyField>of())
    );
    ApplicationClass appClass = new ApplicationClass(
      InspectionApp.class.getName(), "",
      new ReflectionSchemaGenerator().generate(InspectionApp.AConfig.class));
    ArtifactMeta artifactMeta =
      new ArtifactMeta(ArtifactClasses.builder().addPlugins(plugins).addApp(appClass).build());

    String artifactContents = "my artifact contents";
    artifactStore.write(artifactId, artifactMeta, new ByteArrayInputStream(Bytes.toBytes(artifactContents)));

    ArtifactDetail artifactDetail = artifactStore.getArtifact(artifactId);
    assertEqual(artifactId, artifactMeta, artifactContents, artifactDetail);
  }

  @Test(expected = ArtifactAlreadyExistsException.class)
  public void testImmutability() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "myplugins", "1.0.0");
    ArtifactMeta artifactMeta = new ArtifactMeta(ArtifactClasses.builder().build());

    String artifactContents = "abc123";
    // this should work
    try {
      artifactStore.write(artifactId, artifactMeta, new ByteArrayInputStream(Bytes.toBytes(artifactContents)));
    } catch (ArtifactAlreadyExistsException e) {
      Assert.fail();
    }
    // this should throw an exception, since artifacts are immutable
    artifactStore.write(artifactId, artifactMeta, new ByteArrayInputStream(Bytes.toBytes(artifactContents)));
  }

  @Test
  public void testSnapshotMutability() throws Exception {
    ArtifactRange parentArtifacts = new ArtifactRange(
      Constants.DEFAULT_NAMESPACE_ID, "parent", new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0"));
    // write the snapshot once
    PluginClass plugin1 = new PluginClass("atype", "plugin1", "", "c.c.c.plugin1", "cfg",
      ImmutableMap.<String, PluginPropertyField>of());
    PluginClass plugin2 = new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg",
      ImmutableMap.<String, PluginPropertyField>of());
    Id.Artifact artifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "myplugins", "1.0.0-SNAPSHOT");
    ArtifactMeta artifactMeta = new ArtifactMeta(
      ArtifactClasses.builder().addPlugins(plugin1, plugin2).build(), ImmutableSet.of(parentArtifacts));
    artifactStore.write(artifactId, artifactMeta, new ByteArrayInputStream(Bytes.toBytes("abc123")));

    // update meta and jar contents
    artifactMeta = new ArtifactMeta(
      ArtifactClasses.builder().addPlugin(plugin2).build(), ImmutableSet.of(parentArtifacts));
    artifactStore.write(artifactId, artifactMeta, new ByteArrayInputStream(Bytes.toBytes("xyz321")));

    // check the metadata and contents got updated
    ArtifactDetail detail = artifactStore.getArtifact(artifactId);
    assertEqual(artifactId, artifactMeta, "xyz321", detail);

    // check that plugin1 was deleted and plugin2 remains
    Id.Artifact parentArtifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "parent", "1.0.0");
    Assert.assertEquals(ImmutableMap.of(detail.getDescriptor(), plugin2),
                        artifactStore.getPluginClasses(parentArtifactId, plugin2.getType(), plugin2.getName()));
    try {
      artifactStore.getPluginClasses(parentArtifactId, plugin1.getType(), plugin1.getName());
      Assert.fail();
    } catch (PluginNotExistsException e) {
      // expected
    }
  }

  @Test
  public void testNamespaceIsolation() throws Exception {
    Id.Namespace namespace1 = Id.Namespace.from("ns1");
    Id.Namespace namespace2 = Id.Namespace.from("ns2");
    Id.Artifact artifact1 = Id.Artifact.from(namespace1, "myplugins", "1.0.0");
    Id.Artifact artifact2 = Id.Artifact.from(namespace2, "myplugins", "1.0.0");
    String contents1 = "first contents";
    String contents2 = "second contents";
    PluginClass plugin1 =
      new PluginClass("atype", "plugin1", "", "c.c.c.plugin1", "cfg", ImmutableMap.<String, PluginPropertyField>of());
    PluginClass plugin2 =
      new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg", ImmutableMap.<String, PluginPropertyField>of());
    ArtifactMeta meta1 = new ArtifactMeta(ArtifactClasses.builder().addPlugin(plugin1).build());
    ArtifactMeta meta2 = new ArtifactMeta(ArtifactClasses.builder().addPlugin(plugin2).build());

    artifactStore.write(artifact1, meta1, new ByteArrayInputStream(Bytes.toBytes(contents1)));
    artifactStore.write(artifact2, meta2, new ByteArrayInputStream(Bytes.toBytes(contents2)));

    try {
      ArtifactDetail info1 = artifactStore.getArtifact(artifact1);
      ArtifactDetail info2 = artifactStore.getArtifact(artifact2);

      assertEqual(artifact1, meta1, contents1, info1);
      assertEqual(artifact2, meta2, contents2, info2);

      List<ArtifactDetail> namespace1Artifacts = artifactStore.getArtifacts(namespace1);
      List<ArtifactDetail> namespace2Artifacts = artifactStore.getArtifacts(namespace2);
      Assert.assertEquals(1, namespace1Artifacts.size());
      assertEqual(artifact1, meta1, contents1, namespace1Artifacts.get(0));
      Assert.assertEquals(1, namespace2Artifacts.size());
      assertEqual(artifact2, meta2, contents2, namespace2Artifacts.get(0));
    } finally {
      artifactStore.clear(namespace1);
      artifactStore.clear(namespace2);
    }
  }

  @Test
  public void testGetArtifacts() throws Exception {
    // add 1 version of another artifact1
    Id.Artifact artifact1V1 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifact1", "1.0.0");
    String contents1V1 = "first contents v1";
    PluginClass plugin1V1 =
      new PluginClass("atype", "plugin1", "", "c.c.c.plugin1", "cfg", ImmutableMap.<String, PluginPropertyField>of());
    ArtifactMeta meta1V1 = new ArtifactMeta(ArtifactClasses.builder().addPlugin(plugin1V1).build());
    artifactStore.write(artifact1V1, meta1V1, new ByteArrayInputStream(Bytes.toBytes(contents1V1)));

    // add 2 versions of an artifact2
    Id.Artifact artifact2V1 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifact2", "0.1.0");
    Id.Artifact artifact2V2 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifact2", "0.1.1");
    String contents2V1 = "second contents v1";
    String contents2V2 = "second contents v2";
    PluginClass plugin2V1 =
      new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg", ImmutableMap.<String, PluginPropertyField>of());
    PluginClass plugin2V2 =
      new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg", ImmutableMap.<String, PluginPropertyField>of());
    ArtifactMeta meta2V1 = new ArtifactMeta(ArtifactClasses.builder().addPlugin(plugin2V1).build());
    ArtifactMeta meta2V2 = new ArtifactMeta(ArtifactClasses.builder().addPlugin(plugin2V2).build());
    artifactStore.write(artifact2V1, meta2V1, new ByteArrayInputStream(Bytes.toBytes(contents2V1)));
    artifactStore.write(artifact2V2, meta2V2, new ByteArrayInputStream(Bytes.toBytes(contents2V2)));

    // test we get 1 version of artifact1 and 2 versions of artifact2
    List<ArtifactDetail> artifact1Versions =
      artifactStore.getArtifacts(artifact1V1.getNamespace(), artifact1V1.getName());
    Assert.assertEquals(1, artifact1Versions.size());
    assertEqual(artifact1V1, meta1V1, contents1V1, artifact1Versions.get(0));

    List<ArtifactDetail> artifact2Versions =
      artifactStore.getArtifacts(artifact2V1.getNamespace(), artifact2V1.getName());
    Assert.assertEquals(2, artifact2Versions.size());
    assertEqual(artifact2V1, meta2V1, contents2V1, artifact2Versions.get(0));
    assertEqual(artifact2V2, meta2V2, contents2V2, artifact2Versions.get(1));

    // test we get all 3 in the getArtifacts() call for the namespace
    List<ArtifactDetail> artifactVersions = artifactStore.getArtifacts(Constants.DEFAULT_NAMESPACE_ID);
    Assert.assertEquals(3, artifactVersions.size());
    assertEqual(artifact1V1, meta1V1, contents1V1, artifactVersions.get(0));
    assertEqual(artifact2V1, meta2V1, contents2V1, artifactVersions.get(1));
    assertEqual(artifact2V2, meta2V2, contents2V2, artifactVersions.get(2));

    // test get using a range
    // this range should get everything
    ArtifactRange range = new ArtifactRange(
      Constants.DEFAULT_NAMESPACE_ID, "artifact2", new ArtifactVersion("0.1.0"), new ArtifactVersion("0.1.2"));
    artifactVersions = artifactStore.getArtifacts(range);
    Assert.assertEquals(2, artifactVersions.size());
    assertEqual(artifact2V1, meta2V1, contents2V1, artifactVersions.get(0));
    assertEqual(artifact2V2, meta2V2, contents2V2, artifactVersions.get(1));
    // this range should get just v0.1.1
    range = new ArtifactRange(
      Constants.DEFAULT_NAMESPACE_ID, "artifact2", new ArtifactVersion("0.1.1"), new ArtifactVersion("1.0.0"));
    artifactVersions = artifactStore.getArtifacts(range);
    Assert.assertEquals(1, artifactVersions.size());
    assertEqual(artifact2V2, meta2V2, contents2V2, artifactVersions.get(0));
    // this range should get just v0.1.0
    range = new ArtifactRange(
      Constants.DEFAULT_NAMESPACE_ID, "artifact2", new ArtifactVersion("0.0.0"), new ArtifactVersion("0.1.1"));
    artifactVersions = artifactStore.getArtifacts(range);
    Assert.assertEquals(1, artifactVersions.size());
    assertEqual(artifact2V1, meta2V1, contents2V1, artifactVersions.get(0));
  }

  @Test
  public void testGetPlugins() throws Exception {
    ArtifactRange parentArtifacts = new ArtifactRange(
      Constants.DEFAULT_NAMESPACE_ID, "parent", new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0"));
    // we have 2 plugins of type A and 2 plugins of type B
    PluginClass pluginA1 = new PluginClass(
      "A", "p1", "desc", "c.p1", "cfg",
      ImmutableMap.of(
        "threshold", new PluginPropertyField("thresh", "description", "double", true),
        "retry", new PluginPropertyField("retries", "description", "int", false)
      )
    );
    PluginClass pluginA2 = new PluginClass(
      "A", "p2", "desc", "c.p2", "conf",
      ImmutableMap.of(
        "stream", new PluginPropertyField("stream", "description", "string", true)
      )
    );
    PluginClass pluginB1 = new PluginClass(
      "B", "p1", "desc", "c.p1", "cfg",
      ImmutableMap.of(
        "createIfNotExist", new PluginPropertyField("createIfNotExist", "desc", "boolean", false)
      )
    );
    PluginClass pluginB2 = new PluginClass(
      "B", "p2", "desc", "c.p2", "stuff",
      ImmutableMap.of(
        "numer", new PluginPropertyField("numerator", "description", "double", true),
        "denom", new PluginPropertyField("denominator", "description", "double", true)
      )
    );

    // add artifacts

    // not interested in artifact contents for this test, using some dummy value
    byte[] contents = new byte[] { 0 };

    // artifact artifactX-1.0.0 contains plugin A1
    Id.Artifact artifactXv100 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifactX", "1.0.0");
    ArtifactMeta metaXv100 = new ArtifactMeta(
      ArtifactClasses.builder().addPlugin(pluginA1).build(), ImmutableSet.of(parentArtifacts));
    artifactStore.write(artifactXv100, metaXv100, new ByteArrayInputStream(contents));
    ArtifactDescriptor artifactXv100Info = artifactStore.getArtifact(artifactXv100).getDescriptor();

    // artifact artifactX-1.1.0 contains plugin A1
    Id.Artifact artifactXv110 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifactX", "1.1.0");
    ArtifactMeta metaXv110 = new ArtifactMeta(
      ArtifactClasses.builder().addPlugin(pluginA1).build(), ImmutableSet.of(parentArtifacts));
    artifactStore.write(artifactXv110, metaXv110, new ByteArrayInputStream(contents));
    ArtifactDescriptor artifactXv110Info = artifactStore.getArtifact(artifactXv110).getDescriptor();

    // artifact artifactX-2.0.0 contains plugins A1 and A2
    Id.Artifact artifactXv200 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifactX", "2.0.0");
    ArtifactMeta metaXv200 = new ArtifactMeta(
      ArtifactClasses.builder().addPlugins(pluginA1, pluginA2).build(), ImmutableSet.of(parentArtifacts));
    artifactStore.write(artifactXv200, metaXv200, new ByteArrayInputStream(contents));
    ArtifactDescriptor artifactXv200Info = artifactStore.getArtifact(artifactXv200).getDescriptor();

    // artifact artifactY-1.0.0 contains plugin B1
    Id.Artifact artifactYv100 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifactY", "1.0.0");
    ArtifactMeta metaYv100 = new ArtifactMeta(
      ArtifactClasses.builder().addPlugin(pluginB1).build(), ImmutableSet.of(parentArtifacts));
    artifactStore.write(artifactYv100, metaYv100, new ByteArrayInputStream(contents));
    ArtifactDescriptor artifactYv100Info = artifactStore.getArtifact(artifactYv100).getDescriptor();

    // artifact artifactY-2.0.0 contains plugin B2
    Id.Artifact artifactYv200 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifactY", "2.0.0");
    ArtifactMeta metaYv200 = new ArtifactMeta(
      ArtifactClasses.builder().addPlugin(pluginB2).build(), ImmutableSet.of(parentArtifacts));
    artifactStore.write(artifactYv200, metaYv200, new ByteArrayInputStream(contents));
    ArtifactDescriptor artifactYv200Info = artifactStore.getArtifact(artifactYv200).getDescriptor();

    // artifact artifactZ-1.0.0 contains plugins A1 and B1
    Id.Artifact artifactZv100 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifactZ", "1.0.0");
    ArtifactMeta metaZv100 = new ArtifactMeta(
      ArtifactClasses.builder().addPlugins(pluginA1, pluginB1).build(), ImmutableSet.of(parentArtifacts));
    artifactStore.write(artifactZv100, metaZv100, new ByteArrayInputStream(contents));
    ArtifactDescriptor artifactZv100Info = artifactStore.getArtifact(artifactZv100).getDescriptor();

    // artifact artifactZ-2.0.0 contains plugins A1, A2, B1, and B2
    Id.Artifact artifactZv200 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifactZ", "2.0.0");
    ArtifactMeta metaZv200 = new ArtifactMeta(
      ArtifactClasses.builder().addPlugins(pluginA1, pluginA2, pluginB1, pluginB2).build(),
      ImmutableSet.of(parentArtifacts));
    artifactStore.write(artifactZv200, metaZv200, new ByteArrayInputStream(contents));
    ArtifactDescriptor artifactZv200Info = artifactStore.getArtifact(artifactZv200).getDescriptor();

    Id.Artifact parentArtifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "parent", "1.0.0");
    // test getting all plugins in the namespace
    Map<ArtifactDescriptor, List<PluginClass>> expected = Maps.newHashMap();
    expected.put(artifactXv100Info, ImmutableList.of(pluginA1));
    expected.put(artifactXv110Info, ImmutableList.of(pluginA1));
    expected.put(artifactXv200Info, ImmutableList.of(pluginA1, pluginA2));
    expected.put(artifactYv100Info, ImmutableList.of(pluginB1));
    expected.put(artifactYv200Info, ImmutableList.of(pluginB2));
    expected.put(artifactZv100Info, ImmutableList.of(pluginA1, pluginB1));
    expected.put(artifactZv200Info, ImmutableList.of(pluginA1, pluginA2, pluginB1, pluginB2));
    Map<ArtifactDescriptor, List<PluginClass>> actual = artifactStore.getPluginClasses(parentArtifactId);
    Assert.assertEquals(expected, actual);

    // test getting all plugins by namespace and type
    // get all of type A
    expected = Maps.newHashMap();
    expected.put(artifactXv100Info, ImmutableList.of(pluginA1));
    expected.put(artifactXv110Info, ImmutableList.of(pluginA1));
    expected.put(artifactXv200Info, ImmutableList.of(pluginA1, pluginA2));
    expected.put(artifactZv100Info, ImmutableList.of(pluginA1));
    expected.put(artifactZv200Info, ImmutableList.of(pluginA1, pluginA2));
    actual = artifactStore.getPluginClasses(parentArtifactId, "A");
    Assert.assertEquals(expected, actual);
    // get all of type B
    expected = Maps.newHashMap();
    expected.put(artifactYv100Info, ImmutableList.of(pluginB1));
    expected.put(artifactYv200Info, ImmutableList.of(pluginB2));
    expected.put(artifactZv100Info, ImmutableList.of(pluginB1));
    expected.put(artifactZv200Info, ImmutableList.of(pluginB1, pluginB2));
    actual = artifactStore.getPluginClasses(parentArtifactId, "B");
    Assert.assertEquals(expected, actual);

    // test getting plugins by namespace, type, and name
    // get all of type A and name p1
    Map<ArtifactDescriptor, PluginClass> expectedMap = Maps.newHashMap();
    expectedMap.put(artifactXv100Info, pluginA1);
    expectedMap.put(artifactXv110Info, pluginA1);
    expectedMap.put(artifactXv200Info, pluginA1);
    expectedMap.put(artifactZv100Info, pluginA1);
    expectedMap.put(artifactZv200Info, pluginA1);
    Map<ArtifactDescriptor, PluginClass> actualMap = artifactStore.getPluginClasses(parentArtifactId, "A", "p1");
    Assert.assertEquals(expectedMap, actualMap);
    // get all of type A and name p2
    expectedMap = Maps.newHashMap();
    expectedMap.put(artifactXv200Info, pluginA2);
    expectedMap.put(artifactZv200Info, pluginA2);
    actualMap = artifactStore.getPluginClasses(parentArtifactId, "A", "p2");
    Assert.assertEquals(expectedMap, actualMap);
    // get all of type B and name p1
    expectedMap = Maps.newHashMap();
    expectedMap.put(artifactYv100Info, pluginB1);
    expectedMap.put(artifactZv100Info, pluginB1);
    expectedMap.put(artifactZv200Info, pluginB1);
    actualMap = artifactStore.getPluginClasses(parentArtifactId, "B", "p1");
    Assert.assertEquals(expectedMap, actualMap);
    // get all of type B and name p2
    expectedMap = Maps.newHashMap();
    expectedMap.put(artifactYv200Info, pluginB2);
    expectedMap.put(artifactZv200Info, pluginB2);
    actualMap = artifactStore.getPluginClasses(parentArtifactId, "B", "p2");
    Assert.assertEquals(expectedMap, actualMap);
  }

  @Test
  public void testSamePluginDifferentArtifacts() throws Exception {
    ArtifactRange parentArtifacts = new ArtifactRange(
      Constants.DEFAULT_NAMESPACE_ID, "parent", new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0"));
    // add one artifact with a couple plugins
    Id.Artifact artifact1 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "plugins1", "1.0.0");
    List<PluginClass> plugins = ImmutableList.of(
      new PluginClass("atype", "plugin1", "", "c.c.c.plugin1", "cfg", ImmutableMap.<String, PluginPropertyField>of()),
      new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg", ImmutableMap.<String, PluginPropertyField>of())
    );
    ArtifactMeta meta1 = new ArtifactMeta(
      ArtifactClasses.builder().addPlugins(plugins).build(), ImmutableSet.of(parentArtifacts));
    artifactStore.write(artifact1, meta1, new ByteArrayInputStream(Bytes.toBytes("something")));
    ArtifactDescriptor artifact1Info = artifactStore.getArtifact(artifact1).getDescriptor();

    // add a different artifact with the same plugins
    Id.Artifact artifact2 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "plugins2", "1.0.0");
    ArtifactMeta meta2 = new ArtifactMeta(
      ArtifactClasses.builder().addPlugins(plugins).build(), ImmutableSet.of(parentArtifacts));
    artifactStore.write(artifact2, meta2, new ByteArrayInputStream(Bytes.toBytes("something")));
    ArtifactDescriptor artifact2Info = artifactStore.getArtifact(artifact2).getDescriptor();

    Id.Artifact parentArtifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "parent", "1.0.0");
    Map<ArtifactDescriptor, List<PluginClass>> expected = Maps.newHashMap();
    expected.put(artifact1Info, plugins);
    expected.put(artifact2Info, plugins);
    Map<ArtifactDescriptor, List<PluginClass>> actual = artifactStore.getPluginClasses(parentArtifactId);
    Assert.assertEquals(expected, actual);
  }

  // this test tests that when an artifact specifies a range of artifact versions it extends,
  // those versions are honored
  @Test
  public void testPluginParentVersions() throws Exception {
    // write an artifact that extends parent-[1.0.0, 2.0.0)
    Id.Artifact artifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "plugins", "0.1.0");
    Set<ArtifactRange> parentArtifacts = ImmutableSet.of(new ArtifactRange(
      Constants.DEFAULT_NAMESPACE_ID, "parent", new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    List<PluginClass> plugins = ImmutableList.of(
      new PluginClass("atype", "plugin1", "", "c.c.c.plugin1", "cfg", ImmutableMap.<String, PluginPropertyField>of())
    );
    ArtifactMeta meta = new ArtifactMeta(ArtifactClasses.builder().addPlugins(plugins).build(), parentArtifacts);
    artifactStore.write(artifactId, meta, new ByteArrayInputStream(Bytes.toBytes("some contents")));
    ArtifactDescriptor artifactInfo = artifactStore.getArtifact(artifactId).getDescriptor();

    // check ids that are out of range. They should not return anything
    List<Id.Artifact> badIds = Lists.newArrayList(
      // ids that are too low
      Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "parent", "0.9.9"),
      Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "parent", "1.0.0-SNAPSHOT"),
      // ids that are too high
      Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "parent", "2.0.0")
    );
    for (Id.Artifact badId : badIds) {
      Assert.assertTrue(artifactStore.getPluginClasses(badId).isEmpty());
      Assert.assertTrue(artifactStore.getPluginClasses(badId, "atype").isEmpty());
      try {
        artifactStore.getPluginClasses(badId, "atype", "plugin1");
        Assert.fail();
      } catch (PluginNotExistsException e) {
        // expected
      }
    }

    // check ids that are in range return what we expect
    List<Id.Artifact> goodIds = Lists.newArrayList(
      // ids that are too low
      Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "parent", "1.0.0"),
      // ids that are too high
      Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "parent", "1.9.9"),
      Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "parent", "1.99.999"),
      Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "parent", "2.0.0-SNAPSHOT")
    );
    Map<ArtifactDescriptor, List<PluginClass>> expectedPluginsMapList = ImmutableMap.of(artifactInfo, plugins);
    Map<ArtifactDescriptor, PluginClass> expectedPluginsMap = ImmutableMap.of(artifactInfo, plugins.get(0));
    for (Id.Artifact goodId : goodIds) {
      Assert.assertEquals(expectedPluginsMapList, artifactStore.getPluginClasses(goodId));
      Assert.assertEquals(expectedPluginsMapList, artifactStore.getPluginClasses(goodId, "atype"));
      Assert.assertEquals(expectedPluginsMap, artifactStore.getPluginClasses(goodId, "atype", "plugin1"));
    }
  }

  @Category(SlowTests.class)
  @Test
  public void testConcurrentWrite() throws Exception {
    // start up a bunch of threads that will try and write the same artifact at the same time
    // only one of them should be able to write it
    int numThreads = 20;
    final Id.Artifact artifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "abc", "1.0.0");
    final List<String> successfulWriters = Collections.synchronizedList(Lists.<String>newArrayList());

    // use a barrier so they all try and write at the same time
    final CyclicBarrier barrier = new CyclicBarrier(numThreads);
    final CountDownLatch latch = new CountDownLatch(numThreads);
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      final String writer = String.valueOf(i);
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          try {
            barrier.await();
            ArtifactMeta meta = new ArtifactMeta(
              ArtifactClasses.builder()
                .addPlugin(new PluginClass("plugin-type", "plugin" + writer, "", "classname", "cfg",
                  ImmutableMap.<String, PluginPropertyField>of()))
                .build()
            );
            artifactStore.write(artifactId, meta, new ByteArrayInputStream(Bytes.toBytes(writer)));
            successfulWriters.add(writer);
          } catch (InterruptedException | BrokenBarrierException | IOException e) {
            // something went wrong, fail the test
            throw new RuntimeException(e);
          } catch (ArtifactAlreadyExistsException | WriteConflictException e) {
            // these are ok, all but one thread should see this
          } finally {
            latch.countDown();
          }
        }
      });
    }

    // wait for all writers to finish
    latch.await();
    // only one writer should have been able to write
    Assert.assertEquals(1, successfulWriters.size());
    String successfulWriter = successfulWriters.get(0);
    // check that the contents weren't mixed between writers
    ArtifactDetail info = artifactStore.getArtifact(artifactId);
    ArtifactMeta expectedMeta = new ArtifactMeta(
      ArtifactClasses.builder()
        .addPlugin(new PluginClass("plugin-type", "plugin" + successfulWriter, "", "classname", "cfg",
          ImmutableMap.<String, PluginPropertyField>of()))
        .build()
    );
    assertEqual(artifactId, expectedMeta, successfulWriter, info);
  }


  @Category(SlowTests.class)
  @Test
  public void testConcurrentSnapshotWrite() throws Exception {
    final ArtifactRange parentArtifacts = new ArtifactRange(
      Constants.DEFAULT_NAMESPACE_ID, "parent", new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0"));
    // start up a bunch of threads that will try and write the same artifact at the same time
    // only one of them should be able to write it
    int numThreads = 20;
    final Id.Artifact artifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "abc", "1.0.0-SNAPSHOT");

    // use a barrier so they all try and write at the same time
    final CyclicBarrier barrier = new CyclicBarrier(numThreads);
    final CountDownLatch latch = new CountDownLatch(numThreads);
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      final String writer = String.valueOf(i);
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          try {
            barrier.await();
            ArtifactMeta meta = new ArtifactMeta(
              ArtifactClasses.builder()
                .addPlugin(new PluginClass("plugin-type", "plugin" + writer, "", "classname", "cfg",
                  ImmutableMap.<String, PluginPropertyField>of()))
                .build(),
              ImmutableSet.of(parentArtifacts)
            );
            artifactStore.write(artifactId, meta, new ByteArrayInputStream(Bytes.toBytes(writer)));
          } catch (InterruptedException | BrokenBarrierException | ArtifactAlreadyExistsException | IOException e) {
            // something went wrong, fail the test
            throw new RuntimeException(e);
          } catch (WriteConflictException e) {
            // these are ok though unexpected (means couldn't write after a bunch of retries too)
          } finally {
            latch.countDown();
          }
        }
      });
    }

    // wait for all writers to finish
    latch.await();

    // figure out which was the last writer by reading our data. all the writers should have been able to write,
    // and they should have all overwritten each other in a consistent manner
    ArtifactDetail detail = artifactStore.getArtifact(artifactId);
    // figure out the winning writer from the plugin name, which is 'plugin<writer>'
    String pluginName = detail.getMeta().getClasses().getPlugins().iterator().next().getName();
    String winnerWriter = pluginName.substring("plugin".length());

    ArtifactMeta expectedMeta = new ArtifactMeta(
      ArtifactClasses.builder()
        .addPlugin(new PluginClass("plugin-type", "plugin" + winnerWriter, "", "classname", "cfg",
            ImmutableMap.<String, PluginPropertyField>of()))
        .build(),
      ImmutableSet.of(parentArtifacts)
    );
    assertEqual(artifactId, expectedMeta, winnerWriter, detail);

    // check only 1 plugin remains and that its the correct one
    Id.Artifact parentArtifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "parent", "1.0.0");
    Map<ArtifactDescriptor, List<PluginClass>> pluginMap =
      artifactStore.getPluginClasses(parentArtifactId, "plugin-type");
    Map<ArtifactDescriptor, List<PluginClass>> expected = Maps.newHashMap();
    expected.put(detail.getDescriptor(), Lists.newArrayList(
      new PluginClass("plugin-type", "plugin" + winnerWriter, "", "classname", "cfg",
      ImmutableMap.<String, PluginPropertyField>of())));
    Assert.assertEquals(expected, pluginMap);
  }


  private void assertEqual(Id.Artifact expectedId, ArtifactMeta expectedMeta,
                           String expectedContents, ArtifactDetail actual) throws IOException {
    Assert.assertEquals(expectedId.getName(), actual.getDescriptor().getName());
    Assert.assertEquals(expectedId.getVersion(), actual.getDescriptor().getVersion());
    Assert.assertEquals(expectedId.getNamespace().equals(Constants.SYSTEM_NAMESPACE_ID),
                        actual.getDescriptor().isSystem());
    Assert.assertEquals(expectedMeta, actual.getMeta());
    Assert.assertEquals(expectedContents, CharStreams.toString(
      new InputStreamReader(actual.getDescriptor().getLocation().getInputStream(), Charsets.UTF_8)));
  }
}
