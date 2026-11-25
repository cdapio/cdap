/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.service;

import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.app.runtime.spark.SparkClassLoader;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextTest;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.internal.app.PluginWithLocation;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.internal.app.runtime.ProgramClassLoader;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.util.TaskCompletionListener;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultSparkHttpServicePluginContext}.
 */
public class DefaultSparkHttpServicePluginContextTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private ProgramClassLoader programClassLoader;

  @After
  public void tearDown() throws Exception {
    if (programClassLoader != null) {
      programClassLoader.close();
      programClassLoader = null;
    }
    Method unsetMethod = TaskContext$.MODULE$.getClass().getDeclaredMethod("unset");
    unsetMethod.setAccessible(true);
    unsetMethod.invoke(TaskContext$.MODULE$);
  }

  @Test
  public void testCloseAndTaskCompletionAndIoExceptionPropagation() throws Exception {
    File localDataDir = TEMP_FOLDER.newFolder();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    Assert.assertTrue(new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).mkdirs());

    final LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    ProgramId programId = new ProgramId("default", "pluginApp", ProgramType.SPARK, "pluginSpark");
    SparkRuntimeContext runtimeContext = SparkRuntimeContextTest.createRuntimeContext(
        cConf, new Configuration(), programId, RunIds.generate(),
        Collections.emptyList(), null, new Closeable() {
          @Override
          public void close() {
          }
        }, locationFactory, Mockito.mock(AppStateStoreProvider.class));
    programClassLoader = new ProgramClassLoader(
        cConf, TEMP_FOLDER.newFolder(), DefaultSparkHttpServicePluginContextTest.class.getClassLoader());
    Mockito.when(runtimeContext.getProgram().getClassLoader()).thenReturn(programClassLoader);

    DefaultSparkHttpServicePluginContext handlerCtx = new DefaultSparkHttpServicePluginContext(runtimeContext);
    handlerCtx.close();

    // Prepare a deserialized executor context with a TaskContext completion listener and a corrupt plugin jar.
    final List<TaskCompletionListener> completionListeners = new ArrayList<>();
    TaskContext mockTaskContext = Mockito.mock(TaskContext.class);
    Mockito.doAnswer(invocation -> {
      completionListeners.add((TaskCompletionListener) invocation.getArguments()[0]);
      return mockTaskContext;
    }).when(mockTaskContext).addTaskCompletionListener(Mockito.any(TaskCompletionListener.class));

    Method setTaskContext = TaskContext$.MODULE$.getClass().getDeclaredMethod("setTaskContext", TaskContext.class);
    setTaskContext.setAccessible(true);
    setTaskContext.invoke(TaskContext$.MODULE$, mockTaskContext);

    SparkClassLoader sparkClassLoader = new SparkClassLoader(runtimeContext);
    ClassLoader previousCl = ClassLoaders.setContextClassLoader(sparkClassLoader);
    try {
      DefaultSparkHttpServicePluginContext executorCtx = new DefaultSparkHttpServicePluginContext();
      Assert.assertEquals(1, completionListeners.size());

      File deletedPluginDir = TEMP_FOLDER.newFolder();
      Location deletedLocation = Locations.toLocation(deletedPluginDir);
      ArtifactId pluginArtifactId = new ArtifactId("bad-plugin", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
      PluginClass pluginClass = new PluginClass(
          "type", "name", "desc", "com.example.MissingPlugin", "cfg",
          Collections.<String, PluginPropertyField>emptyMap());
      Plugin plugin = new Plugin(Collections.<ArtifactId>emptyList(), pluginArtifactId, pluginClass,
                                 PluginProperties.builder().build());
      PluginWithLocation pluginWithLocation = new PluginWithLocation(plugin, deletedLocation);
      Map<String, PluginWithLocation> pluginsMap = Collections.singletonMap("badPlugin", pluginWithLocation);

      Type pluginsType = new TypeToken<Map<String, PluginWithLocation>>() {
      }.getType();
      byte[] jsonBytes = new GsonBuilder()
          .registerTypeAdapter(Location.class, new TypeAdapter<Location>() {
            @Override
            public void write(JsonWriter out, Location value) throws IOException {
              out.value(value.toURI().getPath());
            }

            @Override
            public Location read(JsonReader in) throws IOException {
              return Locations.getLocationFromAbsolutePath(locationFactory, in.nextString());
            }
          })
          .create()
          .toJson(pluginsMap, pluginsType)
          .getBytes(StandardCharsets.UTF_8);

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeInt(jsonBytes.length);
        oos.write(jsonBytes);
      }
      try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
        executorCtx.readExternal(ois);
      }
      Files.delete(deletedPluginDir.toPath());

      // 1. loadPluginClass("badPlugin") fails expanding the missing jar with IOException -> Throwables.propagate(e).
      try {
        executorCtx.loadPluginClass("badPlugin");
        Assert.fail("Expected RuntimeException wrapping IOException from loadPluginClass");
      } catch (RuntimeException caught) {
        Assert.assertTrue(caught.getCause() instanceof IOException);
      }

      // 2. newPluginInstance("badPlugin") fails expanding the corrupt jar with IOException -> Throwables.propagate(e).
      try {
        executorCtx.newPluginInstance("badPlugin");
        Assert.fail("Expected RuntimeException wrapping IOException from newPluginInstance");
      } catch (RuntimeException caught) {
        Assert.assertTrue(caught.getCause() instanceof IOException);
      }

      // 3. Firing TaskCompletionListener closes pluginInstantiator quietly.
      completionListeners.get(0).onTaskCompletion(mockTaskContext);
    } finally {
      ClassLoaders.setContextClassLoader(previousCl);
    }
  }
}
