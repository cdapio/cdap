/*
 * Copyright © 2023 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.runtime.distributed;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.app.DefaultAppConfigurer;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.twill.NoopTwillRunnerService;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.SparkCompat;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.Configs;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.LogEntry.Level;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.elasticsearch.common.Strings;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DistributedProgramRunnerTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private CConfiguration cConf;
  private File baseDir;
  private Program program;
  private TestDistributedProgramRunner runner;
  private TestTwillPreparer preparer;

  @Before
  public void beforeTest() throws IOException {
    baseDir = TMP_FOLDER.newFolder();
    cConf = CConfiguration.create();
    runner = new TestDistributedProgramRunner(cConf, new Configuration(), baseDir,
        new NoopTwillRunnerService());
    program = createProgram(baseDir);
    preparer = new TestTwillPreparer();
  }

  @Test
  public void testJvmOpts() throws URISyntaxException {
    cConf.set(Constants.AppFabric.PROGRAM_JVM_OPTS, "-Dkey1=val1");
    BasicArguments userArgs =
        new BasicArguments(ImmutableMap.of(SystemArguments.JVM_OPTS, "-Dkey2=val2"));
    ProgramOptions options =
        new SimpleProgramOptions(program.getId(), new BasicArguments(), userArgs);

    runner.setJvmOpts(preparer, options, new URI("file:///etc/cdap/conf/logback.xml"),
        Collections.emptyMap());

    Assert.assertEquals(
        "-Dkey1=val1 -Dkey2=val2 -Dlogback.configurationFile=resources.jar/resources/logback.xml"
            + " -DCDAP_LOG_DIR=<LOG_DIR>", preparer.getJvmOpts());
  }

  @Test
  public void testTwillConfigs() {
    BasicArguments systemArgs =
        new BasicArguments(ImmutableMap.of(SystemArguments.NAMESPACE_CONFIG_PREFIX + "key", "val"));
    BasicArguments userArgs =
        new BasicArguments(ImmutableMap.of(SystemArguments.RUNTIME_CLEANUP_DISABLED, "true"));
    Program program = createProgram(baseDir);
    ProgramOptions options = new SimpleProgramOptions(program.getId(), systemArgs, userArgs);

    runner.setTwillConfigs(preparer, program, options, Collections.emptyMap());

    Map<String, String> config = preparer.getConfig();
    Assert.assertEquals("1", config.get(Configs.Keys.YARN_MAX_APP_ATTEMPTS));
    Assert.assertEquals("val", config.get("key"));
    Assert.assertEquals("true", config.get(SystemArguments.RUNTIME_CLEANUP_DISABLED));
    Assert.assertEquals(program.getNamespaceId(), config.get(ProgramOptionConstants.RUNTIME_NAMESPACE));
  }

  @Test
  public void testSetEnv() {
    ProgramLaunchConfig launchConfig = new ProgramLaunchConfig();
    launchConfig.addExtraEnv(ImmutableMap.of(Constants.SPARK_COMPAT_ENV, SparkCompat.SPARK3_2_12.getCompat()));

    runner.setEnv(preparer, launchConfig);

    Map<String, String> env = preparer.getEnv();
    Assert.assertEquals(ApplicationConstants.LOG_DIR_EXPANSION_VAR, env.get("CDAP_LOG_DIR"));
    Assert.assertEquals(SparkCompat.SPARK3_2_12.getCompat(), env.get(Constants.SPARK_COMPAT_ENV));
  }

  @Test
  public void testSetClassPaths() {
    ProgramLaunchConfig launchConfig = new ProgramLaunchConfig();
    launchConfig.addExtraClasspath(Arrays.asList("path1", "path2"));
    List<String> additionalClassPaths = Arrays.asList("path3", "path4");

    runner.setClassPaths(preparer, launchConfig, additionalClassPaths);

    List<String> classpaths = preparer.getClassPaths();
    Assert.assertTrue(classpaths.contains("path1"));
    Assert.assertTrue(classpaths.contains("path2"));
    Assert.assertTrue(classpaths.contains("path3"));
    Assert.assertTrue(classpaths.contains("path4"));
  }

  @Test
  public void testSetExtraSystemArgs() {
    ProgramLaunchConfig launchConfig = new ProgramLaunchConfig();
    launchConfig.addExtraSystemArgument("key", "val");
    BasicArguments systemArgs =
        new BasicArguments(ImmutableMap.of(ProgramOptionConstants.PROGRAM_JAR_HASH, "xyz"));
    ProgramOptions options =
        new SimpleProgramOptions(program.getId(), systemArgs, new BasicArguments());

    Map<String, String> args = runner.getExtraSystemArgs(launchConfig, program, options);

    Assert.assertEquals("program_xyz.jar",
        args.get(ProgramOptionConstants.PROGRAM_JAR));
    Assert.assertEquals("[\"program_xyz.jar\"]",
        args.get(ProgramOptionConstants.CACHEABLE_FILES));
    Assert.assertEquals(DistributedProgramRunner.CDAP_CONF_FILE_NAME,
        args.get(ProgramOptionConstants.CDAP_CONF_FILE));
    Assert.assertEquals(DistributedProgramRunner.HADOOP_CONF_FILE_NAME,
        args.get(ProgramOptionConstants.HADOOP_CONF_FILE));
    Assert.assertEquals(DistributedProgramRunner.APP_SPEC_FILE_NAME,
        args.get(ProgramOptionConstants.APP_SPEC_FILE));
  }

  @Test
  public void testSetSchedulerQueue() {
    BasicArguments systemArgs =
        new BasicArguments(ImmutableMap.of(Constants.AppFabric.APP_SCHEDULER_QUEUE, "que"));
    ProgramOptions options =
        new SimpleProgramOptions(program.getId(), systemArgs, new BasicArguments());

    runner.setSchedulerQueue(preparer, program, options);

    Assert.assertEquals("que", preparer.getSchedulerQueue());
  }

  @Test
  public void testGetProgramJarNameWithoutJarHash() {
    ProgramOptions options =
        new SimpleProgramOptions(program.getId(), new BasicArguments(), new BasicArguments());
    Assert.assertEquals("program.jar", runner.getProgramJarName(program, options));
  }

  @Test
  public void testGetProgramJarNameWithJarHash() {
    BasicArguments systemArgs =
        new BasicArguments(ImmutableMap.of(ProgramOptionConstants.PROGRAM_JAR_HASH, "1234abc"));
    ProgramOptions options =
        new SimpleProgramOptions(program.getId(), systemArgs, new BasicArguments());
    Assert.assertEquals("program_1234abc.jar", runner.getProgramJarName(program, options));
  }

  private Program createProgram(File baseDir) {
    Application app = new AppWithSparkProgram();
    DefaultAppConfigurer configurer =
        new DefaultAppConfigurer(
            Id.Namespace.DEFAULT,
            new Id.Artifact(Id.Namespace.DEFAULT, "artifact", new ArtifactVersion("0.1")),
            app);
    app.configure(configurer, () -> null);
    ApplicationSpecification appSpec = configurer.createSpecification("app", "1.0");
    return new Program() {
      @Override
      public String getMainClassName() {
        return null;
      }

      @Override
      public <T> Class<T> getMainClass() {
        return null;
      }

      @Override
      public ProgramType getType() {
        return ProgramType.SPARK;
      }

      @Override
      public ProgramId getId() {
        return new ProgramId(getNamespaceId(), getApplicationId(), getType(), getName());
      }

      @Override
      public String getName() {
        return "spark";
      }

      @Override
      public String getNamespaceId() {
        return Id.Namespace.DEFAULT.getId();
      }

      @Override
      public String getApplicationId() {
        return appSpec.getName();
      }

      @Override
      public ApplicationSpecification getApplicationSpecification() {
        return appSpec;
      }

      @Override
      public Location getJarLocation() {
        return Locations.toLocation(new File(baseDir, "program.jar"));
      }

      @Override
      public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
      }

      @Override
      public void close() {}
    };
  }

  static class TestTwillPreparer implements TwillPreparer {
    private Map<String, String> env = new HashMap<>();
    private Map<String, String> config = new HashMap<>();
    private List<String> classPaths = new ArrayList<>();
    private String jvmOpts;
    private String schedulerQueue;

    public Map<String, String> getEnv() {
      return env;
    }

    public Map<String, String> getConfig() {
      return config;
    }

    public List<String> getClassPaths() {
      return classPaths;
    }

    public String getJvmOpts() {
      return jvmOpts;
    }

    public String getSchedulerQueue() {
      return schedulerQueue;
    }

    @Override
    public TwillPreparer withConfiguration(Map<String, String> map) {
      this.config = map;
      return this;
    }

    @Override
    public TwillPreparer withConfiguration(String s, Map<String, String> map) {
      return null;
    }

    @Override
    public TwillPreparer addLogHandler(LogHandler logHandler) {
      return null;
    }

    @Override
    public TwillPreparer setUser(String s) {
      return null;
    }

    @Override
    public TwillPreparer setSchedulerQueue(String s) {
      schedulerQueue = s;
      return this;
    }

    @Override
    public TwillPreparer setJVMOptions(String s) {
      return null;
    }

    @Override
    public TwillPreparer setJVMOptions(String s, String s1) {
      return null;
    }

    @Override
    public TwillPreparer addJVMOptions(String s) {
      if (Strings.isNullOrEmpty(jvmOpts)) {
        jvmOpts = s;
      } else {
        jvmOpts = jvmOpts + " " + s;
      }
      return this;
    }

    @Override
    public TwillPreparer enableDebugging(String... strings) {
      return null;
    }

    @Override
    public TwillPreparer enableDebugging(boolean b, String... strings) {
      return null;
    }

    @Override
    public TwillPreparer withApplicationArguments(String... strings) {
      return null;
    }

    @Override
    public TwillPreparer withApplicationArguments(Iterable<String> iterable) {
      return null;
    }

    @Override
    public TwillPreparer withArguments(String s, String... strings) {
      return null;
    }

    @Override
    public TwillPreparer withArguments(String s, Iterable<String> iterable) {
      return null;
    }

    @Override
    public TwillPreparer withDependencies(Class<?>... classes) {
      return null;
    }

    @Override
    public TwillPreparer withDependencies(Iterable<Class<?>> iterable) {
      return null;
    }

    @Override
    public TwillPreparer withResources(URI... uris) {
      return null;
    }

    @Override
    public TwillPreparer withResources(Iterable<URI> iterable) {
      return null;
    }

    @Override
    public TwillPreparer withClassPaths(String... strings) {
      return null;
    }

    @Override
    public TwillPreparer withClassPaths(Iterable<String> iterable) {
      Iterables.addAll(classPaths, iterable);
      return this;
    }

    @Override
    public TwillPreparer withEnv(Map<String, String> map) {
      this.env.putAll(map);
      return this;
    }

    @Override
    public TwillPreparer withEnv(String s, Map<String, String> map) {
      return null;
    }

    @Override
    public TwillPreparer withApplicationClassPaths(String... strings) {
      return null;
    }

    @Override
    public TwillPreparer withApplicationClassPaths(Iterable<String> iterable) {
      return this;
    }

    @Override
    public TwillPreparer withBundlerClassAcceptor(ClassAcceptor classAcceptor) {
      return null;
    }

    @Override
    public TwillPreparer withMaxRetries(String s, int i) {
      return null;
    }

    @Override
    public TwillPreparer addSecureStore(SecureStore secureStore) {
      return null;
    }

    @Override
    public TwillPreparer setLogLevel(Level level) {
      return null;
    }

    @Override
    public TwillPreparer setLogLevels(Map<String, Level> map) {
      return null;
    }

    @Override
    public TwillPreparer setLogLevels(String s, Map<String, Level> map) {
      return null;
    }

    @Override
    public TwillPreparer setClassLoader(String s) {
      return null;
    }

    @Override
    public TwillController start() {
      return null;
    }

    @Override
    public TwillController start(long l, TimeUnit timeUnit) {
      return null;
    }
  }

  static class TestDistributedProgramRunner extends DistributedProgramRunner {
    TestDistributedProgramRunner(
        CConfiguration cConf, Configuration hConf, File baseDir, TwillRunner twillRunner) {
      super(
          cConf,
          hConf,
          new DefaultImpersonator(cConf, null),
          ClusterMode.ISOLATED,
          twillRunner,
          new LocalLocationFactory(baseDir));
    }

    @Override
    public ProgramController createProgramController(ProgramRunId programRunId,
        TwillController twillController) {
      return null;
    }

    @Override
    protected void setupLaunchConfig(ProgramLaunchConfig launchConfig, Program program,
        ProgramOptions options, CConfiguration cConf, Configuration hConf, File tempDir) {
    }
  }

  static class AppWithSparkProgram extends AbstractApplication {
    @Override
    public void configure() {
      setName("app");
      addSpark(
          new AbstractSpark() {
            @Override
            protected void configure() {
              setName("spark");
              setMainClassName("mainclass");
            }
          });
    }
  }
}
