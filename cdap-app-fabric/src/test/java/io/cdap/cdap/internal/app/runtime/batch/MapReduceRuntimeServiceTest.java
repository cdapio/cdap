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

import com.google.common.io.ByteStreams;
import com.google.inject.Injector;
import io.cdap.cdap.api.mapreduce.MapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link MapReduceRuntimeService}.
 */
public class MapReduceRuntimeServiceTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testGetServiceNameAndExecutorDaemonThreadNaming() throws Exception {
    BasicMapReduceContext context = MapReduceProgramControllerTest.createContext(null);
    try {
      Location jarLocation = Locations.toLocation(tempFolder.newFile("empty.jar"));
      MapReduceRuntimeService service = createService(context, jarLocation);

      Assert.assertEquals("MapReduceRunner-wordCountMr", service.serviceName());

      CountDownLatch latch = new CountDownLatch(1);
      AtomicReference<String> threadName = new AtomicReference<>();
      AtomicBoolean isDaemon = new AtomicBoolean(false);
      service.executor().execute(() -> {
        Thread current = Thread.currentThread();
        threadName.set(current.getName());
        isDaemon.set(current.isDaemon());
        latch.countDown();
      });

      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      Assert.assertEquals("MapReduceRunner-wordCountMr", threadName.get());
      Assert.assertTrue(isDaemon.get());
    } finally {
      context.close();
    }
  }

  @Test
  public void testTriggerShutdownKillsJobAndPropagatesExceptions() throws Exception {
    BasicMapReduceContext context = MapReduceProgramControllerTest.createContext(null);
    try {
      Location jarLocation = Locations.toLocation(tempFolder.newFile("program.jar"));
      MapReduceRuntimeService service = createService(context, jarLocation);

      Job job = Mockito.mock(Job.class);
      Mockito.when(job.isComplete()).thenReturn(false);
      Field jobField = MapReduceRuntimeService.class.getDeclaredField("job");
      jobField.setAccessible(true);
      jobField.set(service, job);

      service.triggerShutdown();
      Mockito.verify(job).killJob();

      IOException ioFailure = new IOException("kill failed");
      Mockito.doThrow(ioFailure).when(job).killJob();
      try {
        service.triggerShutdown();
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (RuntimeException thrown) {
        Assert.assertSame(ioFailure, thrown.getCause());
      }

      IllegalStateException runtimeFailure = new IllegalStateException("kill illegal state");
      Mockito.doThrow(runtimeFailure).when(job).killJob();
      try {
        service.triggerShutdown();
        Assert.fail("Expected IllegalStateException");
      } catch (RuntimeException thrown) {
        Assert.assertSame(runtimeFailure, thrown);
      }
    } finally {
      context.close();
    }
  }

  @Test
  public void testCopyFileToLocationAndCopyProgramJarPreserveBytes() throws Exception {
    byte[] programBytes = "program-jar-binary-payload".getBytes(StandardCharsets.UTF_8);
    File programJarFile = tempFolder.newFile("source-program.jar");
    Files.write(programJarFile.toPath(), programBytes);

    byte[] extraBytes = "extra-plugin-jar-payload".getBytes(StandardCharsets.UTF_8);
    File extraFile = tempFolder.newFile("extra.jar");
    Files.write(extraFile.toPath(), extraBytes);

    BasicMapReduceContext context = MapReduceProgramControllerTest.createContext(null);
    try {
      MapReduceRuntimeService service =
          createService(context, Locations.toLocation(programJarFile));

      LocalLocationFactory locationFactory =
          new LocalLocationFactory(tempFolder.newFolder("target"));
      Location targetDir = locationFactory.create("copied");
      Assert.assertTrue(targetDir.mkdirs());

      Method copyFileMethod = MapReduceRuntimeService.class.getDeclaredMethod(
          "copyFileToLocation", File.class, Location.class);
      copyFileMethod.setAccessible(true);
      Location copiedFile = (Location) copyFileMethod.invoke(service, extraFile, targetDir);
      Assert.assertTrue(copiedFile.exists());
      Assert.assertTrue(copiedFile.getName().endsWith(".jar"));
      try (InputStream in = copiedFile.getInputStream()) {
        Assert.assertArrayEquals(extraBytes, ByteStreams.toByteArray(in));
      }

      Method copyProgramJarMethod = MapReduceRuntimeService.class.getDeclaredMethod(
          "copyProgramJar", Location.class);
      copyProgramJarMethod.setAccessible(true);
      Location copiedProgramJar = (Location) copyProgramJarMethod.invoke(service, targetDir);
      Assert.assertEquals("program.jar", copiedProgramJar.getName());
      Assert.assertTrue(copiedProgramJar.exists());
      try (InputStream in = copiedProgramJar.getInputStream()) {
        Assert.assertArrayEquals(programBytes, ByteStreams.toByteArray(in));
      }
    } finally {
      context.close();
    }
  }

  @Test
  public void testLocalizeUserResourcesDistributedSuccessAndInvalidUriPropagation()
      throws Exception {
    BasicMapReduceContext validContext = MapReduceProgramControllerTest.createContext(null);
    try {
      Location jarLocation = Locations.toLocation(tempFolder.newFile("program.jar"));
      final MapReduceRuntimeService service = createService(validContext, jarLocation);

      Configuration hConf = new Configuration();
      hConf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
      final Job job = Job.getInstance(hConf);

      validContext.localize("dict.txt", URI.create("hdfs://nn/data/dict.txt"), false);
      validContext.localize("bundle.tgz", URI.create("hdfs://nn/data/b.tgz"), true);

      Method localizeMethod = MapReduceRuntimeService.class.getDeclaredMethod(
          "localizeUserResources", Job.class, File.class);
      localizeMethod.setAccessible(true);

      @SuppressWarnings("unchecked")
      Map<String, String> localized =
          (Map<String, String>) localizeMethod.invoke(service, job, tempFolder.newFolder("local"));
      Assert.assertEquals("dict.txt", localized.get("dict.txt"));
      Assert.assertEquals("bundle.tgz", localized.get("bundle.tgz"));
      Assert.assertEquals(1, job.getCacheFiles().length);
      Assert.assertEquals(1, job.getCacheArchives().length);

      // Opaque URI with scheme and null path causes new URI(scheme, authority, path, query, name)
      // to fail with URISyntaxException ("Relative path in absolute URI") when scheme is present
      // and path is relative
      BasicMapReduceContext badUriContext = MapReduceProgramControllerTest.createContext(null);
      try {
        badUriContext.localize("bad-resource", URI.create("urn:opaque-part"), false);
        MapReduceRuntimeService badService = createService(badUriContext, jarLocation);
        try {
          localizeMethod.invoke(badService, job, tempFolder.newFolder("local2"));
          Assert.fail("Expected RuntimeException wrapping URISyntaxException");
        } catch (InvocationTargetException ite) {
          Throwable cause = ite.getTargetException();
          Assert.assertTrue(cause instanceof RuntimeException);
          Assert.assertTrue(cause.getCause() instanceof URISyntaxException);
        }
      } finally {
        badUriContext.close();
      }
    } finally {
      validContext.close();
    }
  }

  private MapReduceRuntimeService createService(
      BasicMapReduceContext context, Location programJarLocation) {
    MapReduceSpecification spec = context.getSpecification();
    return new MapReduceRuntimeService(
        Mockito.mock(Injector.class),
        CConfiguration.create(),
        new Configuration(),
        Mockito.mock(MapReduce.class),
        spec,
        context,
        programJarLocation,
        Mockito.mock(NamespacePathLocator.class),
        ClusterMode.ON_PREMISE,
        Mockito.mock(FieldLineageWriter.class));
  }
}
