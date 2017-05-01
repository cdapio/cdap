/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.app.runtime.distributed.DistributedProgramControllerFactory;
import co.cask.cdap.common.app.MainClassLoader;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.CConfigurationUtil;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.twill.AbortOnTimeoutEventHandler;
import co.cask.cdap.common.twill.HadoopClassExcluder;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.LocalizationUtils;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.internal.app.runtime.spark.SparkUtils;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.security.TokenSecureStoreRenewer;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.security.store.SecureStoreUtils;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
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
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tephra.TxConstants;
import org.apache.twill.api.Configs;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.yarn.YarnSecureStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;


/**
 * Defines the base framework for starting {@link Program} in the cluster.
 */
public abstract class AbstractDistributedProgramRunner implements ProgramRunner, DistributedProgramControllerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDistributedProgramRunner.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .create();
  private static final String HADOOP_CONF_FILE_NAME = "hConf.xml";
  private static final String CDAP_CONF_FILE_NAME = "cConf.xml";
  private static final String APP_SPEC_FILE_NAME = "appSpec.json";
  private static final String LOGBACK_FILE_NAME = "logback.xml";

  protected final YarnConfiguration hConf;
  protected final CConfiguration cConf;
  protected final EventHandler eventHandler;
  private final TwillRunner twillRunner;
  private final TokenSecureStoreRenewer secureStoreRenewer;
  private final Impersonator impersonator;

  /**
   * An interface for launching TwillApplication. Used by sub-classes only.
   */
  protected abstract class ApplicationLauncher {

    protected final List<String> classPaths = new ArrayList<>();
    protected final Set<Class<?>> dependencies = new LinkedHashSet<>();
    protected final Map<String, String> env = new LinkedHashMap<>();

    public ApplicationLauncher addClassPaths(Iterable<String> classpaths) {
      Iterables.addAll(this.classPaths, classpaths);
      return this;
    }

    public ApplicationLauncher addDependencies(Iterable<? extends Class<?>> dependencies) {
      Iterables.addAll(this.dependencies, dependencies);
      return this;
    }

    public ApplicationLauncher addEnvironment(Map<String, String> env) {
      this.env.putAll(env);
      return this;
    }

    /**
     * Starts the given application through Twill.
     *
     * @param twillApplication the application to start
     *
     * @return the {@link TwillController} for the application.
     */
    public abstract TwillController launch(TwillApplication twillApplication);
  }

  protected AbstractDistributedProgramRunner(TwillRunner twillRunner, YarnConfiguration hConf, CConfiguration cConf,
                                             TokenSecureStoreRenewer tokenSecureStoreRenewer,
                                             Impersonator impersonator) {
    this.twillRunner = twillRunner;
    this.hConf = new YarnConfiguration(hConf);
    this.cConf = CConfiguration.copy(cConf);
    this.eventHandler = createEventHandler(cConf);
    this.secureStoreRenewer = tokenSecureStoreRenewer;
    this.impersonator = impersonator;
  }

  protected EventHandler createEventHandler(CConfiguration cConf) {
    return new AbortOnTimeoutEventHandler(cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE));
  }

  /**
   * Validates the options for the program.
   * Subclasses can override this to also validate the options for their sub-programs.
   */
  protected void validateOptions(Program program, ProgramOptions options) {
    // this will throw an exception if the custom tx timeout is invalid
    SystemArguments.validateTransactionTimeout(options.getUserArguments().asMap(), cConf);
  }

  @Override
  public final ProgramController run(final Program program, final ProgramOptions oldOptions) {

    validateOptions(program, oldOptions);

    final File tempDir = DirUtils.createTempDir(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                         cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
    try {
      final String schedulerQueueName = oldOptions.getArguments().getOption(Constants.AppFabric.APP_SCHEDULER_QUEUE);
      if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
        hConf.set(JobContext.QUEUE_NAME, schedulerQueueName);
        LOG.info("Setting scheduler queue to {}", schedulerQueueName);
      }

      // don't have tephra retry in order to give CDAP more control over when to retry and how.
      cConf.set(TxConstants.Service.CFG_DATA_TX_CLIENT_RETRY_STRATEGY, "n-times");
      cConf.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_ATTEMPTS, 0);

      // Unset the hbase.client.retries.number and hbase.rpc.timeout from hConf and cConf so that program container
      // runs with default values for them from hbase-site/hbase-default.
      hConf.unset(Constants.HBase.CLIENT_RETRIES);
      hConf.unset(Constants.HBase.RPC_TIMEOUT);
      cConf.unset(Constants.HBase.CLIENT_RETRIES);
      cConf.unset(Constants.HBase.RPC_TIMEOUT);

      final Map<String, LocalizeResource> localizeResources = new HashMap<>();
      final ProgramOptions options = addArtifactPluginFiles(oldOptions, localizeResources,
                                                            DirUtils.createTempDir(tempDir));

      final List<String> additionalClassPaths = new ArrayList<>();
      List<String> newCConfExtraJars = new ArrayList<>();
      // Add extra jars set in cConf to additionalClassPaths and localizeResources
      for (URI jarURI : CConfigurationUtil.getExtraJars(cConf)) {
        String scheme = jarURI.getScheme();
        LocalizeResource localizeResource = new LocalizeResource(jarURI, false);
        String localizedName = LocalizationUtils.getLocalizedName(jarURI);
        localizeResources.put(localizedName, localizeResource);
        additionalClassPaths.add(localizedName);
        String jarPath = "file".equals(scheme) ? localizedName : jarURI.toString();
        newCConfExtraJars.add(jarPath);
      }

      // Copy config files to local temp, and ask Twill to localize it to container.
      // What Twill does is to save those files in HDFS and keep using them during the lifetime of application.
      // Twill will manage the cleanup of those files in HDFS.
      localizeResources.put(HADOOP_CONF_FILE_NAME,
                            new LocalizeResource(saveHConf(hConf, File.createTempFile("hConf", ".xml", tempDir))));
      localizeResources.put(CDAP_CONF_FILE_NAME,
                            new LocalizeResource(saveCConf(cConf, File.createTempFile("cConf", ".xml", tempDir),
                                                           newCConfExtraJars)));

      // Localize the program jar
      Location programJarLocation = program.getJarLocation();
      final String programJarName = programJarLocation.getName();
      localizeResources.put(programJarName, new LocalizeResource(program.getJarLocation().toURI(), false));

      // Localize the app spec
      localizeResources.put(APP_SPEC_FILE_NAME,
                            new LocalizeResource(saveAppSpec(program,
                                                             File.createTempFile("appSpec", ".json", tempDir))));

      final URI logbackURI = getLogBackURI(program, tempDir);
      if (logbackURI != null) {
        // Localize the logback xml
        localizeResources.put(LOGBACK_FILE_NAME, new LocalizeResource(logbackURI, false));
      }

      final String programOptions = GSON.toJson(options, ProgramOptions.class);

      // Obtains and add the HBase delegation token as well (if in non-secure mode, it's a no-op)
      // Twill would also ignore it if it is not running in secure mode.
      // The HDFS token should already obtained by Twill.

      Callable<ProgramController> callable = new Callable<ProgramController>() {
        @Override
        public ProgramController call() throws Exception {

          return launch(program, options, localizeResources, tempDir, new ApplicationLauncher() {
            @Override
            public TwillController launch(TwillApplication twillApplication) {
              TwillPreparer twillPreparer = twillRunner.prepare(twillApplication);
              Iterables.addAll(additionalClassPaths, classPaths);
              if (options.isDebug()) {
                twillPreparer.enableDebugging();
              }

              logProgramStart(program, options);
              LOG.info("Starting {} with debugging enabled: {}, programOptions: {}, and logback: {}",
                       program.getId(), options.isDebug(), programOptions, logbackURI);
              // Add scheduler queue name if defined
              if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
                LOG.info("Setting scheduler queue for app {} as {}", program.getId(), schedulerQueueName);
                twillPreparer.setSchedulerQueue(schedulerQueueName);
              }

              if (logbackURI != null) {
                twillPreparer.addJVMOptions("-Dlogback.configurationFile=" + LOGBACK_FILE_NAME);
              }
              setLogLevels(twillPreparer, program, options);

              String logLevelConf = cConf.get(Constants.COLLECT_APP_CONTAINER_LOG_LEVEL).toUpperCase();
              if ("OFF".equals(logLevelConf)) {
                twillPreparer.withConfiguration(Collections.singletonMap(Configs.Keys.LOG_COLLECTION_ENABLED, "false"));
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
                twillPreparer.addLogHandler(
                  new ApplicationLogHandler(new PrinterLogHandler(new PrintWriter(System.out)), logLevel));
              }

              // Add secure tokens
              if (User.isHBaseSecurityEnabled(hConf) || UserGroupInformation.isSecurityEnabled()) {
                twillPreparer.addSecureStore(YarnSecureStore.create(secureStoreRenewer.createCredentials()));
              }

              Iterable<Class<?>> dependencies = Iterables.concat(
                Collections.singletonList(HBaseTableUtilFactory.getHBaseTableUtilClass()),
                Collections.singletonList(new HBaseDDLExecutorFactory(cConf, hConf).get().getClass()),
                getKMSSecureStore(cConf), this.dependencies);

              Iterable<String> yarnAppClassPath = Arrays.asList(
                hConf.getTrimmedStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));

              // Setup the environment for the container
              Map<String, String> env = new LinkedHashMap<>(this.env);
              // This is for logback xml
              env.put("CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);

              URL sparkAssemblyJar = null;
              try {
                sparkAssemblyJar = SparkUtils.locateSparkAssemblyJar().toURI().toURL();
              } catch (Exception e) {
                // It's ok if spark is not available. No need to log anything
              }

              final URL finalSparkAssemblyJar = sparkAssemblyJar;
              twillPreparer
                .withDependencies(dependencies)
                .withClassPaths(Iterables.concat(additionalClassPaths, yarnAppClassPath))
                .withEnv(env)
                .withApplicationClassPaths(yarnAppClassPath)
                .withBundlerClassAcceptor(new HadoopClassExcluder() {
                  @Override
                  public boolean accept(String className, URL classUrl, URL classPathUrl) {
                    // Exclude both hadoop and spark classes.
                    if (finalSparkAssemblyJar != null && finalSparkAssemblyJar.equals(classPathUrl)) {
                      return false;
                    }
                    return super.accept(className, classUrl, classPathUrl)
                      && !className.startsWith("org.apache.spark.");
                  }
                })
                .withApplicationArguments(
                  "--" + RunnableOptions.JAR, programJarName,
                  "--" + RunnableOptions.HADOOP_CONF_FILE, HADOOP_CONF_FILE_NAME,
                  "--" + RunnableOptions.CDAP_CONF_FILE, CDAP_CONF_FILE_NAME,
                  "--" + RunnableOptions.APP_SPEC_FILE, APP_SPEC_FILE_NAME,
                  "--" + RunnableOptions.PROGRAM_OPTIONS, programOptions,
                  "--" + RunnableOptions.PROGRAM_ID, GSON.toJson(program.getId())
                )
                // Use the MainClassLoader for class rewriting
                .setClassLoader(MainClassLoader.class.getName());

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
                twillController = twillPreparer.start(cConf.getLong(Constants.AppFabric.PROGRAM_MAX_START_SECONDS),
                                                      TimeUnit.SECONDS);
              } finally {
                ClassLoaders.setContextClassLoader(oldClassLoader);
              }
              return addCleanupListener(twillController, program, tempDir);
            }
          });
        }
      };

      return impersonator.doAs(program.getId(), callable);

    } catch (Exception e) {
      deleteDirectory(tempDir);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Set up Logging Context so the Log is tagged correctly for the Program.
   * Reset the context once done.
   */
  private void logProgramStart(Program program, ProgramOptions options) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContext(program.getNamespaceId(), program.getApplicationId(),
                                             program.getName(), program.getType(),
                                             ProgramRunners.getRunId(options).getId(),
                                             options.getArguments().asMap());
    Cancellable saveContextCancellable =
      LoggingContextAccessor.setLoggingContext(loggingContext);
    String userArguments = Joiner.on(", ").withKeyValueSeparator("=").join(options.getUserArguments());
    LOG.info("Starting {} Program '{}' with Arguments [{}]", program.getType(), program.getName(), userArguments);
    saveContextCancellable.cancel();
  }

  private static List<? extends Class<?>> getKMSSecureStore(CConfiguration cConf) {
    if (SecureStoreUtils.isKMSBacked(cConf) && SecureStoreUtils.isKMSCapable()) {
      return Collections.singletonList(SecureStoreUtils.getKMSSecureStore());
    } else {
      return Collections.emptyList();
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

  protected TwillPreparer setLogLevels(TwillPreparer twillPreparer, Program program, ProgramOptions options) {
    Map<String, String> logLevels = SystemArguments.getLogLevels(options.getUserArguments().asMap());
    if (logLevels.isEmpty()) {
      return twillPreparer;
    }
    return twillPreparer.setLogLevels(transformLogLevels(logLevels));
  }

  protected Map<String, LogEntry.Level> transformLogLevels(Map<String, String> logLevels) {
    return Maps.transformValues(logLevels, new Function<String, LogEntry.Level>() {
      @Override
      public LogEntry.Level apply(String input) {
        return LogEntry.Level.valueOf(input.toUpperCase());
      }
    });
  }

  private File saveCConf(CConfiguration conf, File file, List<String> newExtraJars) throws IOException {
    // Unsetting the runtime extension directory as the necessary extension jars should be shipped to the container
    // by the distributed ProgramRunner.
    CConfiguration copied = CConfiguration.copy(conf);
    copied.unset(Constants.AppFabric.RUNTIME_EXT_DIR);

    // Set the CFG_LOCAL_DATA_DIR to a relative path as the data directory for the container should be relative to the
    // container directory
    copied.set(Constants.CFG_LOCAL_DATA_DIR, "data");

    copied.setStrings(Constants.AppFabric.PROGRAM_CONTAINER_DIST_JARS,
                      newExtraJars.toArray(new String[newExtraJars.size()]));
    try (Writer writer = Files.newWriter(file, Charsets.UTF_8)) {
      copied.writeXml(writer);
    }
    return file;
  }

  private File saveAppSpec(Program program, File file) throws IOException {
    try (Writer writer = Files.newWriter(file, Charsets.UTF_8)) {
      GSON.toJson(program.getApplicationSpecification(), writer);
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
