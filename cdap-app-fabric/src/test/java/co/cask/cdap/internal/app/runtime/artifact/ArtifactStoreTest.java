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
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.SlowTests;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
  public static void set() throws Exception {
    artifactStore = AppFabricTestHelper.getInjector().getInstance(ArtifactStore.class);
  }

  @After
  public void cleanup() throws IOException {
    artifactStore.clear(Constants.DEFAULT_NAMESPACE_ID);
  }

  @Test
  public void testAddGetSingleArtifact() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "myplugins", "1.0.0");
    List<PluginClass> plugins = ImmutableList.of(
      new PluginClass("atype", "plugin1", "", "c.c.c.plugin1", "cfg", ImmutableMap.<String, PluginPropertyField>of()),
      new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg", ImmutableMap.<String, PluginPropertyField>of())
    );
    ArtifactMeta artifactMeta = new ArtifactMeta(plugins);

    String artifactContents = "my artifact contents";
    artifactStore.write(artifactId, artifactMeta, new ByteArrayInputStream(Bytes.toBytes(artifactContents)));

    ArtifactInfo artifactInfo = artifactStore.getArtifact(artifactId);
    assertEqual(artifactId, artifactMeta, artifactContents, artifactInfo);
  }

  @Test(expected = ArtifactAlreadyExistsException.class)
  public void testImmutability() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "myplugins", "1.0.0");
    ArtifactMeta artifactMeta = new ArtifactMeta(ImmutableList.<PluginClass>of());

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
  public void testNamespaceIsolation() throws Exception {
    Id.Namespace namespace1 = Id.Namespace.from("ns1");
    Id.Namespace namespace2 = Id.Namespace.from("ns2");
    Id.Artifact artifact1 = Id.Artifact.from(namespace1, "myplugins", "1.0.0");
    Id.Artifact artifact2 = Id.Artifact.from(namespace2, "myplugins", "1.0.0");
    String contents1 = "first contents";
    String contents2 = "second contents";
    List<PluginClass> plugins1 = ImmutableList.of(
      new PluginClass("atype", "plugin1", "", "c.c.c.plugin1", "cfg", ImmutableMap.<String, PluginPropertyField>of())
    );
    List<PluginClass> plugins2 = ImmutableList.of(
      new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg", ImmutableMap.<String, PluginPropertyField>of())
    );
    ArtifactMeta meta1 = new ArtifactMeta(plugins1);
    ArtifactMeta meta2 = new ArtifactMeta(plugins2);

    artifactStore.write(artifact1, meta1, new ByteArrayInputStream(Bytes.toBytes(contents1)));
    artifactStore.write(artifact2, meta2, new ByteArrayInputStream(Bytes.toBytes(contents2)));

    try {
      ArtifactInfo info1 = artifactStore.getArtifact(artifact1);
      ArtifactInfo info2 = artifactStore.getArtifact(artifact2);

      assertEqual(artifact1, meta1, contents1, info1);
      assertEqual(artifact2, meta2, contents2, info2);

      List<ArtifactInfo> namespace1Artifacts = artifactStore.getArtifacts(namespace1);
      List<ArtifactInfo> namespace2Artifacts = artifactStore.getArtifacts(namespace2);
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
    List<PluginClass> plugins1V1 = ImmutableList.of(
      new PluginClass("atype", "plugin1", "", "c.c.c.plugin1", "cfg", ImmutableMap.<String, PluginPropertyField>of())
    );
    ArtifactMeta meta1V1 = new ArtifactMeta(plugins1V1);
    artifactStore.write(artifact1V1, meta1V1, new ByteArrayInputStream(Bytes.toBytes(contents1V1)));

    // add 2 versions of an artifact2
    Id.Artifact artifact2V1 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifact2", "0.1.0");
    Id.Artifact artifact2V2 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifact2", "0.1.1");
    String contents2V1 = "second contents v1";
    String contents2V2 = "second contents v2";
    List<PluginClass> plugins2V1 = ImmutableList.of(
      new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg", ImmutableMap.<String, PluginPropertyField>of())
    );
    List<PluginClass> plugins2V2 = ImmutableList.of(
      new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg", ImmutableMap.<String, PluginPropertyField>of())
    );
    ArtifactMeta meta2V1 = new ArtifactMeta(plugins2V1);
    ArtifactMeta meta2V2 = new ArtifactMeta(plugins2V2);
    artifactStore.write(artifact2V1, meta2V1, new ByteArrayInputStream(Bytes.toBytes(contents2V1)));
    artifactStore.write(artifact2V2, meta2V2, new ByteArrayInputStream(Bytes.toBytes(contents2V2)));

    // test we get 1 version of artifact1 and 2 versions of artifact2
    List<ArtifactInfo> artifact1Versions =
      artifactStore.getArtifacts(artifact1V1.getNamespace(), artifact1V1.getName());
    Assert.assertEquals(1, artifact1Versions.size());
    assertEqual(artifact1V1, meta1V1, contents1V1, artifact1Versions.get(0));

    List<ArtifactInfo> artifact2Versions =
      artifactStore.getArtifacts(artifact2V1.getNamespace(), artifact2V1.getName());
    Assert.assertEquals(2, artifact2Versions.size());
    assertEqual(artifact2V1, meta2V1, contents2V1, artifact2Versions.get(0));
    assertEqual(artifact2V2, meta2V2, contents2V2, artifact2Versions.get(1));

    // test we get all 3 in the getArtifacts() call for the namespace
    List<ArtifactInfo> artifactVersions = artifactStore.getArtifacts(Constants.DEFAULT_NAMESPACE_ID);
    Assert.assertEquals(3, artifactVersions.size());
    assertEqual(artifact1V1, meta1V1, contents1V1, artifactVersions.get(0));
    assertEqual(artifact2V1, meta2V1, contents2V1, artifactVersions.get(1));
    assertEqual(artifact2V2, meta2V2, contents2V2, artifactVersions.get(2));
  }

  @Test
  public void testGetPlugins() throws Exception {
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
    ArtifactMeta metaXv100 = new ArtifactMeta(ImmutableList.of(pluginA1));
    artifactStore.write(artifactXv100, metaXv100, new ByteArrayInputStream(contents));

    // artifact artifactX-1.1.0 contains plugin A1
    Id.Artifact artifactXv110 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifactX", "1.1.0");
    ArtifactMeta metaXv110 = new ArtifactMeta(ImmutableList.of(pluginA1));
    artifactStore.write(artifactXv110, metaXv110, new ByteArrayInputStream(contents));

    // artifact artifactX-2.0.0 contains plugins A1 and A2
    Id.Artifact artifactXv200 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifactX", "2.0.0");
    ArtifactMeta metaXv200 = new ArtifactMeta(ImmutableList.of(pluginA1, pluginA2));
    artifactStore.write(artifactXv200, metaXv200, new ByteArrayInputStream(contents));

    // artifact artifactY-1.0.0 contains plugin B1
    Id.Artifact artifactYv100 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifactY", "1.0.0");
    ArtifactMeta metaYv100 = new ArtifactMeta(ImmutableList.of(pluginB1));
    artifactStore.write(artifactYv100, metaYv100, new ByteArrayInputStream(contents));

    // artifact artifactY-2.0.0 contains plugin B2
    Id.Artifact artifactYv200 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifactY", "2.0.0");
    ArtifactMeta metaYv200 = new ArtifactMeta(ImmutableList.of(pluginB2));
    artifactStore.write(artifactYv200, metaYv200, new ByteArrayInputStream(contents));

    // artifact artifactZ-1.0.0 contains plugins A1 and B1
    Id.Artifact artifactZv100 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifactZ", "1.0.0");
    ArtifactMeta metaZv100 = new ArtifactMeta(ImmutableList.of(pluginA1, pluginB1));
    artifactStore.write(artifactZv100, metaZv100, new ByteArrayInputStream(contents));

    // artifact artifactZ-2.0.0 contains plugins A1, A2, B1, and B2
    Id.Artifact artifactZv200 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "artifactZ", "2.0.0");
    ArtifactMeta metaZv200 = new ArtifactMeta(ImmutableList.of(pluginA1, pluginA2, pluginB1, pluginB2));
    artifactStore.write(artifactZv200, metaZv200, new ByteArrayInputStream(contents));

    // test getting all plugins in the namespace
    Map<Id.Artifact, List<PluginClass>> expected = Maps.newHashMap();
    expected.put(artifactXv100, ImmutableList.of(pluginA1));
    expected.put(artifactXv110, ImmutableList.of(pluginA1));
    expected.put(artifactXv200, ImmutableList.of(pluginA1, pluginA2));
    expected.put(artifactYv100, ImmutableList.of(pluginB1));
    expected.put(artifactYv200, ImmutableList.of(pluginB2));
    expected.put(artifactZv100, ImmutableList.of(pluginA1, pluginB1));
    expected.put(artifactZv200, ImmutableList.of(pluginA1, pluginA2, pluginB1, pluginB2));
    Map<Id.Artifact, List<PluginClass>> actual = artifactStore.getPluginClasses(Constants.DEFAULT_NAMESPACE_ID);
    Assert.assertEquals(expected, actual);

    // test getting all plugins by namespace and type
    // get all of type A
    expected = Maps.newHashMap();
    expected.put(artifactXv100, ImmutableList.of(pluginA1));
    expected.put(artifactXv110, ImmutableList.of(pluginA1));
    expected.put(artifactXv200, ImmutableList.of(pluginA1, pluginA2));
    expected.put(artifactZv100, ImmutableList.of(pluginA1));
    expected.put(artifactZv200, ImmutableList.of(pluginA1, pluginA2));
    actual = artifactStore.getPluginClasses(Constants.DEFAULT_NAMESPACE_ID, "A");
    Assert.assertEquals(expected, actual);
    // get all of type B
    expected = Maps.newHashMap();
    expected.put(artifactYv100, ImmutableList.of(pluginB1));
    expected.put(artifactYv200, ImmutableList.of(pluginB2));
    expected.put(artifactZv100, ImmutableList.of(pluginB1));
    expected.put(artifactZv200, ImmutableList.of(pluginB1, pluginB2));
    actual = artifactStore.getPluginClasses(Constants.DEFAULT_NAMESPACE_ID, "B");
    Assert.assertEquals(expected, actual);

    // test getting plugins by namespace, type, and name
    // get all of type A and name p1
    expected = Maps.newHashMap();
    expected.put(artifactXv100, ImmutableList.of(pluginA1));
    expected.put(artifactXv110, ImmutableList.of(pluginA1));
    expected.put(artifactXv200, ImmutableList.of(pluginA1));
    expected.put(artifactZv100, ImmutableList.of(pluginA1));
    expected.put(artifactZv200, ImmutableList.of(pluginA1));
    actual = artifactStore.getPluginClasses(Constants.DEFAULT_NAMESPACE_ID, "A", "p1");
    Assert.assertEquals(expected, actual);
    // get all of type A and name p2
    expected = Maps.newHashMap();
    expected.put(artifactXv200, ImmutableList.of(pluginA2));
    expected.put(artifactZv200, ImmutableList.of(pluginA2));
    actual = artifactStore.getPluginClasses(Constants.DEFAULT_NAMESPACE_ID, "A", "p2");
    Assert.assertEquals(expected, actual);
    // get all of type B and name p1
    expected = Maps.newHashMap();
    expected.put(artifactYv100, ImmutableList.of(pluginB1));
    expected.put(artifactZv100, ImmutableList.of(pluginB1));
    expected.put(artifactZv200, ImmutableList.of(pluginB1));
    actual = artifactStore.getPluginClasses(Constants.DEFAULT_NAMESPACE_ID, "B", "p1");
    Assert.assertEquals(expected, actual);
    // get all of type B and name p2
    expected = Maps.newHashMap();
    expected.put(artifactYv200, ImmutableList.of(pluginB2));
    expected.put(artifactZv200, ImmutableList.of(pluginB2));
    actual = artifactStore.getPluginClasses(Constants.DEFAULT_NAMESPACE_ID, "B", "p2");
    Assert.assertEquals(expected, actual);

    // test some things that should return empty maps
    // namespace with nothing in it
    Assert.assertTrue(artifactStore.getPluginClasses(Id.Namespace.from("something")).isEmpty());
    // type with no plugins
    Assert.assertTrue(artifactStore.getPluginClasses(Constants.DEFAULT_NAMESPACE_ID, "C").isEmpty());
    // name with no plugins
    Assert.assertTrue(artifactStore.getPluginClasses(Constants.DEFAULT_NAMESPACE_ID, "A", "p3").isEmpty());
  }

  @Test
  public void testSamePluginDifferentArtifacts() throws Exception {
    // add one artifact with a couple plugins
    Id.Artifact artifact1 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "plugins1", "1.0.0");
    List<PluginClass> plugins = ImmutableList.of(
      new PluginClass("atype", "plugin1", "", "c.c.c.plugin1", "cfg", ImmutableMap.<String, PluginPropertyField>of()),
      new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg", ImmutableMap.<String, PluginPropertyField>of())
    );
    ArtifactMeta meta1 = new ArtifactMeta(plugins);
    artifactStore.write(artifact1, meta1, new ByteArrayInputStream(Bytes.toBytes("something")));

    // add a different artifact with the same plugins
    Id.Artifact artifact2 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "plugins2", "1.0.0");
    ArtifactMeta meta2 = new ArtifactMeta(plugins);
    artifactStore.write(artifact2, meta2, new ByteArrayInputStream(Bytes.toBytes("something")));

    Map<Id.Artifact, List<PluginClass>> expected = Maps.newHashMap();
    expected.put(artifact1, plugins);
    expected.put(artifact2, plugins);
    Map<Id.Artifact, List<PluginClass>> actual = artifactStore.getPluginClasses(Constants.DEFAULT_NAMESPACE_ID);
    Assert.assertEquals(expected, actual);
  }

  @Category(SlowTests.class)
  @Test
  public void testConcurrentAdd() throws Exception {
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
            ArtifactMeta meta = new ArtifactMeta(ImmutableList.of(new PluginClass(
              "plugin-type", "plugin" + writer, "", "classname", "cfg",
              ImmutableMap.<String, PluginPropertyField>of())));
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
    ArtifactInfo info = artifactStore.getArtifact(artifactId);
    ArtifactMeta expectedMeta = new ArtifactMeta(ImmutableList.of(new PluginClass(
      "plugin-type", "plugin" + successfulWriter, "", "classname", "cfg",
      ImmutableMap.<String, PluginPropertyField>of())));
    assertEqual(artifactId, expectedMeta, String.valueOf(successfulWriter), info);
  }

  private void assertEqual(Id.Artifact expectedId, ArtifactMeta expectedMeta,
                           String expectedContents, ArtifactInfo actual) throws IOException {
    Assert.assertEquals(expectedId, actual.getId());
    Assert.assertEquals(expectedMeta, actual.getMeta());
    Assert.assertEquals(expectedContents, CharStreams.toString(
      new InputStreamReader(actual.getLocation().getInputStream(), Charsets.UTF_8)));
  }
}
