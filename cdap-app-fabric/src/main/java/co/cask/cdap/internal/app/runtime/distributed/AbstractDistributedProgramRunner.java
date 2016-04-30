/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.twill.AbortOnTimeoutEventHandler;
import co.cask.cdap.common.twill.HadoopClassExcluder;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.security.TokenSecureStoreUpdater;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;


/**
 * Defines the base framework for starting {@link Program} in the cluster.
 */
public abstract class AbstractDistributedProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDistributedProgramRunner.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .create();

  private final TwillRunner twillRunner;
  protected final YarnConfiguration hConf;
  protected final CConfiguration cConf;
  protected final EventHandler eventHandler;
  private final TokenSecureStoreUpdater secureStoreUpdater;

  /**
   * An interface for launching TwillApplication. Used by sub-classes only.
   */
  protected abstract class ApplicationLauncher {

    /**
     * Starts the given application through Twill.
     *
     * @param twillApplication the application to start
     *
     * @return the {@link TwillController} for the application.
     */
    public TwillController launch(TwillApplication twillApplication) {
      return launch(twillApplication, Collections.<String>emptyList(), Collections.<Class<?>>emptyList());
    }

    /**
     * Starts the given application through Twill with extra classpaths appended to the end of the classpath of
     * the runnables inside the applications.
     *
     * @param twillApplication the application to start
     * @param extraClassPaths to append
     *
     * @return the {@link TwillController} for the application.
     * @see TwillPreparer#withClassPaths(Iterable)
     */
    public TwillController launch(TwillApplication twillApplication, String...extraClassPaths) {
      return launch(twillApplication, Arrays.asList(extraClassPaths), Collections.<Class<?>>emptyList());
    }

    /**
     * Starts the given application through Twill with extra classpaths appended to the end of the classpath of
     * the runnables inside the applications.
     *
     * @param twillApplication the application to start
     * @param extraClassPaths to append
     *
     * @return the {@link TwillController} for the application.
     * @see TwillPreparer#withClassPaths(Iterable)
     */
    public abstract TwillController launch(TwillApplication twillApplication, Iterable<String> extraClassPaths,
                                           Iterable<? extends Class<?>> extraDependencies);
  }

  protected AbstractDistributedProgramRunner(TwillRunner twillRunner, YarnConfiguration hConf, CConfiguration cConf,
                                             TokenSecureStoreUpdater tokenSecureStoreUpdater) {
    this.twillRunner = twillRunner;
    this.hConf = hConf;
    this.cConf = cConf;
    this.eventHandler = createEventHandler(cConf);
    this.secureStoreUpdater = tokenSecureStoreUpdater;
  }

  protected EventHandler createEventHandler(CConfiguration cConf) {
    return new AbortOnTimeoutEventHandler(cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE));
  }

  @Override
  public final ProgramController run(final Program program, final ProgramOptions oldOptions) {
    final String schedulerQueueName = oldOptions.getArguments().getOption(Constants.AppFabric.APP_SCHEDULER_QUEUE);
    final File tempDir = DirUtils.createTempDir(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                         cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
    try {
      if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
        hConf.set(JobContext.QUEUE_NAME, schedulerQueueName);
        LOG.info("Setting scheduler queue to {}", schedulerQueueName);
      }

      Map<String, LocalizeResource> localizeResources = new HashMap<>();
      final ProgramOptions options = addArtifactPluginFiles(oldOptions, localizeResources,
                                                            DirUtils.createTempDir(tempDir));

      // Copy config files and program jar to local temp, and ask Twill to localize it to container.
      // What Twill does is to save those files in HDFS and keep using them during the lifetime of application.
      // Twill will manage the cleanup of those files in HDFS.
      localizeResources.put("hConf.xml",
                            new LocalizeResource(saveHConf(hConf, File.createTempFile("hConf", ".xml", tempDir))));
      localizeResources.put("cConf.xml",
                            new LocalizeResource(saveCConf(cConf, File.createTempFile("cConf", ".xml", tempDir))));

      final URI logbackURI = getLogBackURI(program, tempDir);
      final String programOptions = GSON.toJson(options);

      // Obtains and add the HBase delegation token as well (if in non-secure mode, it's a no-op)
      // Twill would also ignore it if it is not running in secure mode.
      // The HDFS token should already obtained by Twill.
      return launch(program, options, localizeResources, tempDir, new ApplicationLauncher() {
        @Override
        public TwillController launch(TwillApplication twillApplication, Iterable<String> extraClassPaths,
                                      Iterable<? extends Class<?>> extraDependencies) {
          TwillPreparer twillPreparer = twillRunner.prepare(twillApplication);
          // TODO: CDAP-5506. It's a bit hacky to set a Spark environment here. However, we always launch
          // Spark using YARN and it is needed for both Workflow and Spark runner. We need to set it
          // because inside Spark code, it will set and unset the SPARK_YARN_MODE system properties, causing
          // fork in distributed mode not working. Setting it in the environment, which Spark uses for defaults,
          // so it can't be unset by Spark
          twillPreparer.withEnv(Collections.singletonMap("SPARK_YARN_MODE", "true"));
          if (options.isDebug()) {
            LOG.info("Starting {} with debugging enabled, programOptions: {}, and logback: {}",
                     program.getId(), programOptions, logbackURI);
            twillPreparer.enableDebugging();
          }
          // Add scheduler queue name if defined
          if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
            LOG.info("Setting scheduler queue for app {} as {}", program.getId(), schedulerQueueName);
            twillPreparer.setSchedulerQueue(schedulerQueueName);
          }
          if (logbackURI != null) {
            twillPreparer.withResources(logbackURI);
          }

          String logLevelConf = cConf.get(Constants.COLLECT_APP_CONTAINER_LOG_LEVEL).toUpperCase();
          if ("OFF".equals(logLevelConf)) {
            twillPreparer.addJVMOptions("-Dtwill.disable.kafka=true");
          } else {
            LogEntry.Level logLevel = LogEntry.Level.ERROR;
            if ("ALL".equals(logLevelConf)) {
              logLevel = LogEntry.Level.TRACE;
            } else {
              try {
                logLevel = LogEntry.Level.valueOf(logLevelConf.toUpperCase());
              } catch (Exception e) {
                LOG.warn("Invalid application container log level {}. Defaulting to ERROR.", logLevelConf);
              }
            }
            twillPreparer.addLogHandler(new ApplicationLogHandler(new PrinterLogHandler(new PrintWriter(System.out)),
                                                                  logLevel));
          }

          String yarnAppClassPath = hConf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                           Joiner.on(",").join(YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));
          // Add secure tokens
          if (User.isHBaseSecurityEnabled(hConf) || UserGroupInformation.isSecurityEnabled()) {
            // TokenSecureStoreUpdater.update() ignores parameters
            twillPreparer.addSecureStore(secureStoreUpdater.update(null, null));
          }

          Iterable<Class<?>> dependencies = Iterables.concat(
            Collections.singletonList(HBaseTableUtilFactory.getHBaseTableUtilClass()), extraDependencies
          );
          twillPreparer
            .withDependencies(dependencies)
            .withClassPaths(Iterables.concat(extraClassPaths, Splitter.on(',').trimResults()
              .split(hConf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH, ""))))
            .withApplicationClassPaths(Splitter.on(",").trimResults().split(yarnAppClassPath))
            .withBundlerClassAcceptor(new HadoopClassExcluder() {
              @Override
              public boolean accept(String className, URL classUrl, URL classPathUrl) {
                // Exclude both hadoop and spark classes.
                return super.accept(className, classUrl, classPathUrl) && !className.startsWith("org.apache.spark.");
              }
            })
            .withApplicationArguments(
              String.format("--%s", RunnableOptions.JAR), program.getJarLocation().getName(),
              String.format("--%s", RunnableOptions.PROGRAM_OPTIONS), programOptions
            );

          TwillController twillController;
          // Change the context classloader to the combine classloader of this ProgramRunner and
          // all the classloaders of the dependencies classes so that Twill can trace classes.
          ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(new CombineClassLoader(
            AbstractDistributedProgramRunner.this.getClass().getClassLoader(),
            Iterables.transform(dependencies, new Function<Class<?>, ClassLoader>() {
              @Override
              public ClassLoader apply(Class<?> input) {
                return input.getClassLoader();
              }
            })));
          try {
            twillController = twillPreparer.start();
          } finally {
            ClassLoaders.setContextClassLoader(oldClassLoader);
          }
          return addCleanupListener(twillController, program, tempDir);
        }
      });
    } catch (Exception e) {
      deleteDirectory(tempDir);
      throw Throwables.propagate(e);
    }
  }

  private ProgramOptions addArtifactPluginFiles(ProgramOptions options, Map<String, LocalizeResource> localizeResources,
                                                File tempDir) throws IOException {
    Arguments systemArgs = options.getArguments();
    if (!systemArgs.hasOption(ProgramOptionConstants.PLUGIN_DIR)) {
      return options;
    }

    File localDir = new File(systemArgs.getOption(ProgramOptionConstants.PLUGIN_DIR));
    File archiveFile = new File(tempDir, "artifacts.jar");
    BundleJarUtil.createJar(localDir, archiveFile);

    // Localize plugins to two files, one expanded into a directory, one not.
    localizeResources.put("artifacts", new LocalizeResource(archiveFile, true));
    localizeResources.put("artifacts_archive.jar", new LocalizeResource(archiveFile, false));

    Map<String, String> newSystemArgs = Maps.newHashMap(systemArgs.asMap());
    newSystemArgs.put(ProgramOptionConstants.PLUGIN_DIR, "artifacts");
    newSystemArgs.put(ProgramOptionConstants.PLUGIN_ARCHIVE, "artifacts_archive.jar");
    return new SimpleProgramOptions(options.getName(), new BasicArguments(newSystemArgs),
                                    options.getUserArguments(), options.isDebug());
  }

  /**
   * Returns a {@link URI} for the logback.xml file to be localized to container and available in the container
   * classpath.
   */
  @Nullable
  private URI getLogBackURI(Program program, File tempDir) throws IOException, URISyntaxException {
    URL logbackURL = program.getClassLoader().getResource("logback.xml");
    if (logbackURL != null) {
      return logbackURL.toURI();
    }
    URL resource = getClass().getClassLoader().getResource("logback-container.xml");
    if (resource == null) {
      return null;
    }
    // Copy the template
    File logbackFile = new File(tempDir, "logback.xml");
    Files.copy(Resources.newInputStreamSupplier(resource), logbackFile);
    return logbackFile.toURI();
  }

  /**
   * Sub-class overrides this method to launch the twill application.
   *
   * @param program the program to launch
   * @param options the options for the program
   * @param localizeResources a mutable map for adding extra resources to localize
   * @param tempDir a temporary directory for this launch. Sub-classes can use it to create resources for localization
   *                which require cleanup after launching completed
   * @param launcher an {@link ApplicationLauncher} to actually launching the program
   */
  protected abstract ProgramController launch(Program program, ProgramOptions options,
                                              Map<String, LocalizeResource> localizeResources,
                                              File tempDir,
                                              ApplicationLauncher launcher);


  private File saveHConf(Configuration conf, File file) throws IOException {
    try (Writer writer = Files.newWriter(file, Charsets.UTF_8)) {
      conf.writeXml(writer);
    }
    return file;
  }

  private File saveCConf(CConfiguration conf, File file) throws IOException {
    // Unsettting the runtime extension directory as the necessary extension jars should be shipped to the container
    // by the distributed ProgramRunner.
    CConfiguration copied = CConfiguration.copy(conf);
    copied.unset(Constants.AppFabric.RUNTIME_EXT_DIR);
    try (Writer writer = Files.newWriter(file, Charsets.UTF_8)) {
      copied.writeXml(writer);
    }
    return file;
  }

  /**
   * Deletes the given directory recursively. Only log if there is {@link IOException}.
   */
  private void deleteDirectory(File directory) {
    try {
      DirUtils.deleteDirectoryContents(directory);
    } catch (IOException e) {
      LOG.warn("Failed to delete directory {}", directory, e);
    }
  }

  /**
   * Adds a listener to the given TwillController to delete local temp files when the program has started/terminated.
   * The local temp files could be removed once the program is started, since Twill would keep the files in
   * HDFS and no long needs the local temp files once program is started.
   *
   * @return The same TwillController instance.
   */
  private TwillController addCleanupListener(TwillController controller,
                                             final Program program, final File tempDir) {

    final AtomicBoolean deleted = new AtomicBoolean(false);
    Runnable cleanup = new Runnable() {

      public void run() {
        if (!deleted.compareAndSet(false, true)) {
          return;
        }
        LOG.debug("Cleanup tmp files for {}: {}", program.getId(), tempDir);
        deleteDirectory(tempDir);
      }};
    controller.onRunning(cleanup, Threads.SAME_THREAD_EXECUTOR);
    controller.onTerminated(cleanup, Threads.SAME_THREAD_EXECUTOR);
    return controller;
  }

  private static final class ApplicationLogHandler implements LogHandler {

    private final LogHandler delegate;
    private final LogEntry.Level logLevel;

    private ApplicationLogHandler(LogHandler delegate, LogEntry.Level logLevel) {
      this.delegate = delegate;
      this.logLevel = logLevel;
    }

    @Override
    public void onLog(LogEntry logEntry) {
      if (logEntry.getLogLevel().ordinal() <= logLevel.ordinal()) {
        delegate.onLog(logEntry);
      }
    }
  }
}
