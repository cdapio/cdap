/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link MapReduceClassLoader}.
 */
public class MapReduceClassLoaderTest {

  private static final Gson gson = new GsonBuilder()
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
      .create();

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testGetTaskContextProviderMemoizesStartsAndStopsOnClose() {
    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = createConfiguredHadoopConf();
    Injector injector = Mockito.mock(Injector.class, invocation -> {
      if ("getInstance".equals(invocation.getMethod().getName())
          && invocation.getArguments().length == 1
          && invocation.getArguments()[0] instanceof Class) {
        return Mockito.mock((Class<?>) invocation.getArguments()[0]);
      }
      return Mockito.RETURNS_DEFAULTS.answer(invocation);
    });

    MapReduceClassLoader loader = new MapReduceClassLoader(
        injector, cConf, hConf, getClass().getClassLoader(), Collections.emptyMap(), null);

    MapReduceTaskContextProvider firstProvider = loader.getTaskContextProvider();
    MapReduceTaskContextProvider secondProvider = loader.getTaskContextProvider();

    Assert.assertSame(firstProvider, secondProvider);
    Assert.assertEquals(Service.State.RUNNING, firstProvider.state());

    loader.close();
    Assert.assertEquals(Service.State.TERMINATED, firstProvider.state());

    // Subsequent close when already TERMINATED should be a safe no-op
    loader.close();
  }

  @Test
  public void testCloseWithoutTaskContextProviderInitializedIsNoOp() {
    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = createConfiguredHadoopConf();
    Injector injector = Mockito.mock(Injector.class);

    MapReduceClassLoader loader = new MapReduceClassLoader(
        injector, cConf, hConf, getClass().getClassLoader(), Collections.emptyMap(), null);
    loader.close();
  }

  @Test
  public void testCreateProgramClassLoaderPropagatesIoException() throws Exception {
    File missingJar = new File(tempFolder.getRoot(), "nonexistent-program.jar");
    Configuration hConf = createConfiguredHadoopConf();
    hConf.set("cdap.mapreduce.program.jar.uri", missingJar.toURI().toASCIIString());
    MapReduceContextConfig contextConfig = new MapReduceContextConfig(hConf);

    Class<?> parametersClass = Class.forName(
        MapReduceClassLoader.class.getName() + "$Parameters");
    Constructor<?> ctor = parametersClass.getDeclaredConstructor(MapReduceContextConfig.class);
    ctor.setAccessible(true);

    try {
      ctor.newInstance(contextConfig);
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (InvocationTargetException ite) {
      Throwable cause = ite.getTargetException();
      Assert.assertTrue(cause instanceof RuntimeException);
      Assert.assertTrue(cause.getCause() instanceof IOException);
    }
  }

  private Configuration createConfiguredHadoopConf() {
    Configuration hConf = new Configuration();
    ProgramId programId = NamespaceId.DEFAULT.app("mrApp").mr("mrProg");
    ProgramOptions options = new SimpleProgramOptions(
        programId,
        new BasicArguments(ImmutableMap.of(
            ProgramOptionConstants.RUN_ID, RunIds.generate().getId())),
        new BasicArguments());
    hConf.set("cdap.mapreduce.program.id", gson.toJson(programId));
    hConf.set("cdap.mapreduce.program.options", gson.toJson(options, ProgramOptions.class));
    return hConf;
  }
}
