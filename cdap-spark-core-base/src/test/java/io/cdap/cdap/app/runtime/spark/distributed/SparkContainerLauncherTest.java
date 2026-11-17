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

package io.cdap.cdap.app.runtime.spark.distributed;

import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.spark.SparkHttpServiceHandlerSpecification;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextTest;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeUtils;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
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
import java.io.PrintStream;
import java.io.Writer;
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
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

/**
 * Unit tests for {@link SparkContainerLauncher}.
 */
public class SparkContainerLauncherTest {

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

  private static final String LAUNCH_MARKER_PROP = "cdap.test.spark.container.launched";

  private SecurityManager savedSecurityManager;
  private ClassLoader savedContextClassLoader;
  private Thread.UncaughtExceptionHandler savedUncaughtHandler;
  private PrintStream savedOut;
  private PrintStream savedErr;

  /**
   * Delegate main class invoked inside {@link SparkContainerLauncher#launch}.
   */
  public static final class DummySparkDelegateMain {
    /**
     * Entry point invoked by {@link SparkContainerLauncher#launch}.
     */
    public static void main(String[] args) {
      System.setProperty(LAUNCH_MARKER_PROP, args.length > 0 ? args[0] : "empty");
    }
  }

  @Before
  public void saveProcessGlobals() {
    savedSecurityManager = System.getSecurityManager();
    savedContextClassLoader = Thread.currentThread().getContextClassLoader();
    savedUncaughtHandler = Thread.getDefaultUncaughtExceptionHandler();
    savedOut = System.out;
    savedErr = System.err;
    System.clearProperty(LAUNCH_MARKER_PROP);
  }

  @After
  public void restoreProcessGlobals() {
    System.setSecurityManager(savedSecurityManager);
    Thread.currentThread().setContextClassLoader(savedContextClassLoader);
    Thread.setDefaultUncaughtExceptionHandler(savedUncaughtHandler);
    System.setOut(savedOut);
    System.setErr(savedErr);
    System.clearProperty(LAUNCH_MARKER_PROP);
    System.clearProperty(RemoteClient.RUNTIME_SERVICE_ROUTING_BASE_URI);
    deleteIfExists(new File("cConf.xml"));
    deleteIfExists(new File("hConf.xml"));
    deleteIfExists(new File("program.jar"));
    deleteIfExists(new File("program.jar.expanded.zip"));
    deleteIfExists(new File(SparkRuntimeUtils.PYSPARK_PORT_FILE_NAME));
    deleteIfExists(new File(SparkRuntimeUtils.PYSPARK_SECRET_FILE_NAME));
  }

  @Test
  public void testLaunchExecutesMainAndClosesSparkRuntimeContextQuietly() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL, "http://127.0.0.1:12345/");

    try (Writer writer = Files.newBufferedWriter(new File("cConf.xml").toPath(), StandardCharsets.UTF_8)) {
      cConf.writeXml(writer);
    }

    ProgramId programId = new ProgramId("default", "launcherApp", ProgramType.SPARK, "launcherSpark");
    Configuration hConf = new Configuration();
    SparkRuntimeContext templateContext = SparkRuntimeContextTest.createRuntimeContext(
        cConf, hConf, programId, RunIds.generate(),
        Collections.<SparkHttpServiceHandlerSpecification>emptyList(), null, new Closeable() {
          @Override
          public void close() {
          }
        });
    SparkRuntimeContextTest.initContextConfig(hConf, templateContext);

    Map<String, String> sysArgs = new HashMap<>();
    sysArgs.put(ProgramOptionConstants.RUN_ID, templateContext.getRunId().getId());
    sysArgs.put(ProgramOptionConstants.CLUSTER_MODE, ClusterMode.ISOLATED.name());
    sysArgs.put(ProgramOptionConstants.ARTIFACT_ID, "default:testArtifact:1.0.0");
    Map<String, String> userArgs = Collections.singletonMap(
        SystemArguments.RUNTIME_MONITOR_TYPE, RuntimeMonitorType.URL.name());
    SimpleProgramOptions isolatedOptions = new SimpleProgramOptions(
        programId, new BasicArguments(sysArgs), new BasicArguments(userArgs));
    hConf.set("cdap.spark.program.options",
              new GsonBuilder()
                  .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
                  .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
                  .create()
                  .toJson(isolatedOptions, ProgramOptions.class));

    try (FileOutputStream fos = new FileOutputStream(new File("hConf.xml"))) {
      hConf.writeXml(fos);
    }
    try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(new File("program.jar")))) {
      jos.putNextEntry(new JarEntry("META-INF/dummy.txt"));
      jos.closeEntry();
    }
    new File("program.jar.expanded.zip").mkdirs();

    SparkContainerLauncher.launch(DummySparkDelegateMain.class.getName(), new String[]{"ok-arg"}, false, null);
    Assert.assertEquals("ok-arg", System.getProperty(LAUNCH_MARKER_PROP));
    Assert.assertTrue(System.getSecurityManager() instanceof SparkRuntimeSecurityManager);
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
