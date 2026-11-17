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

package io.cdap.cdap.app.runtime.spark;

import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.spark.SparkHttpServiceHandlerSpecification;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.Writer;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

/**
 * Unit tests for {@link SparkRuntimeContext}.
 */
public class SparkRuntimeContextProviderTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Rule
  public final TestRule cwdLockRule = (base, description) -> new Statement() {
    @Override
    public void evaluate() throws Throwable {
      synchronized (System.class) {
        File lockFile = new File(System.getProperty("java.io.tmpdir"), "cdap-spark-cwd-test.lock");
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(lockFile, "rw");
             java.nio.channels.FileChannel ch = raf.getChannel();
             java.nio.channels.FileLock ignored = ch.lock()) {
          base.evaluate();
        }
      }
    }
  };

  @After
  public void resetStaticState() throws Exception {
    Field ctxField = SparkRuntimeContextProvider.class.getDeclaredField("sparkRuntimeContext");
    ctxField.setAccessible(true);
    ctxField.set(null, null);
    SparkRuntimeContextProvider.setMasterEnvName(null);
    System.clearProperty(RemoteClient.RUNTIME_SERVICE_ROUTING_BASE_URI);
    deleteIfExists(new File(SparkRuntimeContextProvider.CCONF_FILE_NAME));
    deleteIfExists(new File(SparkRuntimeContextProvider.HCONF_FILE_NAME));
    deleteIfExists(new File(SparkRuntimeContextProvider.PROGRAM_JAR_NAME));
    deleteIfExists(new File(SparkRuntimeContextProvider.PROGRAM_JAR_EXPANDED_NAME));
  }

  @Test
  public void testGetFromSparkClassLoaderAndFailurePropagation() {
    SparkRuntimeContext expected = SparkRuntimeContextTest.createRuntimeContext(
        CConfiguration.create(), new Configuration());
    SparkClassLoader sparkClassLoader = new SparkClassLoader(expected);
    ClassLoader previous = ClassLoaders.setContextClassLoader(sparkClassLoader);
    try {
      Assert.assertSame(expected, SparkRuntimeContextProvider.get());
    } finally {
      ClassLoaders.setContextClassLoader(previous);
    }

    // Without SparkClassLoader and without localized hConf.xml/cConf.xml, createIfNotExists() fails
    // and propagates the unchecked exception directly via Throwables.propagate(e).
    try {
      SparkRuntimeContextProvider.get();
      Assert.fail("Expected RuntimeException when localized config files are absent");
    } catch (RuntimeException caught) {
      Assert.assertNotNull(caught);
    }
  }

  @Test
  public void testCreateIfNotExistsStartsAndStopsCoreServicesInIsolatedMode() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL, "http://127.0.0.1:12345/");

    File cConfFile = new File(SparkRuntimeContextProvider.CCONF_FILE_NAME);
    try (Writer writer = Files.newBufferedWriter(cConfFile.toPath(), StandardCharsets.UTF_8)) {
      cConf.writeXml(writer);
    }

    ProgramId programId = new ProgramId("default", "isolatedApp", ProgramType.SPARK, "isolatedSpark");
    SparkRuntimeContext templateCtx = SparkRuntimeContextTest.createRuntimeContext(
        cConf, new Configuration(), programId, RunIds.generate(),
        Collections.<SparkHttpServiceHandlerSpecification>emptyList(), null, new Closeable() {
          @Override
          public void close() {
            // no-op
          }
        });

    Map<String, String> sysArgs = new HashMap<>();
    sysArgs.put(ProgramOptionConstants.RUN_ID, templateCtx.getRunId().getId());
    sysArgs.put(ProgramOptionConstants.CLUSTER_MODE, ClusterMode.ISOLATED.name());
    sysArgs.put(ProgramOptionConstants.ARTIFACT_ID, "default:testArtifact:1.0.0");
    Map<String, String> userArgs = Collections.singletonMap(
        SystemArguments.RUNTIME_MONITOR_TYPE, RuntimeMonitorType.URL.name());
    SimpleProgramOptions isolatedOptions = new SimpleProgramOptions(
        programId, new BasicArguments(sysArgs), new BasicArguments(userArgs));

    Configuration hConf = new Configuration();
    hConf.set("cdap.spark.program.id", ApplicationSpecificationAdapter.addTypeAdapters(
        new GsonBuilder()).create().toJson(programId));
    new SparkRuntimeContextConfig(hConf).set(
        SparkRuntimeContextTest.createRuntimeContext(
            cConf, hConf, programId, isolatedOptions.getArguments().hasOption(ProgramOptionConstants.RUN_ID)
                ? RunIds.fromString(isolatedOptions.getArguments().getOption(ProgramOptionConstants.RUN_ID))
                : RunIds.generate(),
            Collections.<SparkHttpServiceHandlerSpecification>emptyList(), null, new Closeable() {
              @Override
              public void close() {
              }
            }),
        null);
    // Ensure CLUSTER_MODE=ISOLATED and URL monitor type are preserved in serialized program options inside hConf.
    hConf.set("cdap.spark.program.options",
              new GsonBuilder()
                  .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
                  .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
                  .create()
                  .toJson(isolatedOptions, ProgramOptions.class));

    File hConfFile = new File(SparkRuntimeContextProvider.HCONF_FILE_NAME);
    try (FileOutputStream fos = new FileOutputStream(hConfFile)) {
      hConf.writeXml(fos);
    }

    File programJar = new File(SparkRuntimeContextProvider.PROGRAM_JAR_NAME);
    try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(programJar))) {
      jos.putNextEntry(new JarEntry("META-INF/dummy.txt"));
      jos.closeEntry();
    }
    File expandedDir = new File(SparkRuntimeContextProvider.PROGRAM_JAR_EXPANDED_NAME);
    expandedDir.mkdirs();

    SparkRuntimeContext created = SparkRuntimeContextProvider.get();
    Assert.assertNotNull(created);
    Assert.assertEquals(programId, created.getProgram().getId());

    // Closing invokes the provider closeable which calls service.stopAndWait() on started core services.
    created.close();
  }

  private static void deleteIfExists(File file) {
    if (file.isDirectory()) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          deleteIfExists(child);
        }
      }
    }
    if (file.exists()) {
      file.delete();
    }
  }
}
