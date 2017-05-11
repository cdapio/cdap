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

import co.cask.cdap.api.Resources;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
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
import co.cask.cdap.internal.app.runtime.distributed.ProgramTwillApplication.RunnableResource;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.security.TokenSecureStoreRenewer;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.security.store.SecureStoreUtils;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tephra.TxConstants;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.Configs;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
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
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
public abstract class DistributedProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedProgramRunner.class);
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
  protected final TokenSecureStoreRenewer secureStoreRenewer;
  private final TwillRunner twillRunner;
  private final Impersonator impersonator;

  protected DistributedProgramRunner(TwillRunner twillRunner, YarnConfiguration hConf, CConfiguration cConf,
                                     TokenSecureStoreRenewer tokenSecureStoreRenewer,
                                     Impersonator impersonator) {
    this.twillRunner = twillRunner;
    this.hConf = hConf;
    this.cConf = cConf;
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


  /**
   * Creates a {@link ProgramController} for the given program that was launched as a Twill application.
   *
   * @param twillController the {@link TwillController} to interact with the twill application
   * @param programDescriptor information for the Program being launched
   * @param runId the run id of the particular execution
   * @return a new instance of {@link ProgramController}.
   */
  protected abstract ProgramController createProgramController(TwillController twillController,
                                                               ProgramDescriptor programDescriptor, RunId runId);

  /**
   * Creates the set of {@link RunnableResource} for a given program that are going to be launched in the
   * {@link ProgramTwillApplication}.
   *
   * @param program the program to be launched
   * @param programOptions options for the program to be launched
   * @return A {@link Map} from runnable name to {@link RunnableResource}
   */
  protected abstract Map<String, RunnableResource> getRunnables(Program program, ProgramOptions programOptions);

  /**
   * Returns the launch order for the given set of runnables. By default is an empty list, which means no ordering.
   *
   * @param runnables the runnables to be launched
   * @return a {@link Iterable} that gives the start order of runnables. The runnable names in the same set will
   *         be started in the same order
   */
  protected Iterable<Set<String>> getRunnableLaunchOrder(Map<String, RunnableResource> runnables) {
    return Collections.emptyList();
  }

  /**
   * Returns the set of extra resources that need to be localized to Twill containers.
   *
   * @param program the program to be launched in Twill container.
   * @param tempDir a local directory for creating temporary resources. Sub-classes doesn't need to perform any
   *                cleanup of this directory.
   * @return A {@link Map} from localized name to {@link LocalizeResource}.
   */
  protected Map<String, LocalizeResource> getExtraLocalizeResources(Program program, File tempDir) {
    return Collections.emptyMap();
  }

  /**
   * Prepare launching of the Twill application.
   *
   * @param program the program to be launched
   * @param preparer the {@link TwillPreparer} for extra setup of the launching context
   */
  protected void prepareLaunch(Program program, TwillPreparer preparer) {
    // no-op
  }

  /**
   * Returns the {@link ClassAcceptor} used for application bundle creation.
   * By default it returns the {@link HadoopClassExcluder}.
   */
  protected ClassAcceptor getBundlerClassAcceptor(Program program) {
    return new HadoopClassExcluder();
  }

  /**
   * Returns a {@link ResourceSpecification} created from the given {@link Resources} and number of instances.
   */
  protected final ResourceSpecification createResourceSpec(Resources resources, int instances) {
    return ResourceSpecification.Builder.with()
      .setVirtualCores(resources.getVirtualCores())
      .setMemory(resources.getMemoryMB(), ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();
  }

  @Override
  public final ProgramController run(final Program program, ProgramOptions oldOptions) {
    validateOptions(program, oldOptions);

    final CConfiguration cConf = createContainerCConf(program, this.cConf);
    final Configuration hConf = createContainerHConf(program, this.hConf);

    final File tempDir = DirUtils.createTempDir(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                         cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
    try {
      final Map<String, LocalizeResource> localizeResources = new HashMap<>();
      final ProgramOptions options = addArtifactPluginFiles(oldOptions, localizeResources,
                                                            DirUtils.createTempDir(tempDir));

      final List<String> additionalClassPaths = new ArrayList<>();
      addContainerJars(cConf, localizeResources, additionalClassPaths);

      // Copy config files to local temp, and ask Twill to localize it to container.
      // What Twill does is to save those files in HDFS and keep using them during the lifetime of application.
      // Twill will manage the cleanup of those files in HDFS.
      localizeResources.put(HADOOP_CONF_FILE_NAME,
                            new LocalizeResource(saveHConf(hConf, File.createTempFile("hConf", ".xml", tempDir))));
      localizeResources.put(CDAP_CONF_FILE_NAME,
                            new LocalizeResource(saveCConf(cConf, File.createTempFile("cConf", ".xml", tempDir))));

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

      // All extra localize resources needed by the program runner
      localizeResources.putAll(getExtraLocalizeResources(program, tempDir));

      Callable<ProgramController> callable = new Callable<ProgramController>() {
        @Override
        public ProgramController call() throws Exception {
          Map<String, RunnableResource> runnables = getRunnables(program, options);
          ProgramTwillApplication twillApplication = new ProgramTwillApplication(program.getId(),
                                                                                 runnables,
                                                                                 getRunnableLaunchOrder(runnables),
                                                                                 localizeResources,
                                                                                 createEventHandler(cConf));

          ProgramTwillPreparer twillPreparer = new ProgramTwillPreparer(twillRunner.prepare(twillApplication));
          if (options.isDebug()) {
            twillPreparer.enableDebugging();
          }

          logProgramStart(program, options);

          String serializedOptions = GSON.toJson(options, ProgramOptions.class);
          LOG.info("Starting {} with debugging enabled: {}, programOptions: {}, and logback: {}",
                   program.getId(), options.isDebug(), serializedOptions, logbackURI);

          // Add scheduler queue name if defined
          String schedulerQueueName = options.getArguments().getOption(Constants.AppFabric.APP_SCHEDULER_QUEUE);
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

          // Setup the environment for the container logback.xml
          twillPreparer.withEnv(Collections.singletonMap("CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR));

          // Add dependencies
          twillPreparer
            .withDependencies(HBaseTableUtilFactory.getHBaseTableUtilClass())
            .withDependencies(new HBaseDDLExecutorFactory(cConf, hConf).get().getClass());
          addKMSSecureStoreClass(cConf, twillPreparer);

          // Add the additional classes to the classpath that comes from the container jar setting
          twillPreparer.withClassPaths(additionalClassPaths);

          // Let sub-class to setup the preparer first
          prepareLaunch(program, twillPreparer);

          // The Yarn app classpath goes last
          List<String> yarnAppClassPath = Arrays.asList(
            hConf.getTrimmedStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));
          twillPreparer
            .withApplicationClassPaths(yarnAppClassPath)
            .withClassPaths(yarnAppClassPath)
            .withBundlerClassAcceptor(getBundlerClassAcceptor(program))
            .withApplicationArguments(
              "--" + RunnableOptions.JAR, programJarName,
              "--" + RunnableOptions.HADOOP_CONF_FILE, HADOOP_CONF_FILE_NAME,
              "--" + RunnableOptions.CDAP_CONF_FILE, CDAP_CONF_FILE_NAME,
              "--" + RunnableOptions.APP_SPEC_FILE, APP_SPEC_FILE_NAME,
              "--" + RunnableOptions.PROGRAM_OPTIONS, serializedOptions,
              "--" + RunnableOptions.PROGRAM_ID, GSON.toJson(program.getId())
            )
            // Use the MainClassLoader for class rewriting
            .setClassLoader(MainClassLoader.class.getName());

          TwillController twillController;
          // Change the context classloader to the combine classloader of this ProgramRunner and
          // all the classloaders of the dependencies classes so that Twill can trace classes.
          ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(new CombineClassLoader(
            DistributedProgramRunner.this.getClass().getClassLoader(),
            Iterables.transform(twillPreparer.getDependencies(), new Function<Class<?>, ClassLoader>() {
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
          return createProgramController(addCleanupListener(twillController, program, tempDir),
                                         new ProgramDescriptor(program.getId(), program.getApplicationSpecification()),
                                         ProgramRunners.getRunId(options));
        }
      };

      return impersonator.doAs(program.getId(), callable);

    } catch (Exception e) {
      deleteDirectory(tempDir);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates the {@link CConfiguration} to be used in the program container.
   */
  protected CConfiguration createContainerCConf(Program program, CConfiguration cConf) {
    CConfiguration result = CConfiguration.copy(cConf);
    // don't have tephra retry in order to give CDAP more control over when to retry and how.
    result.set(TxConstants.Service.CFG_DATA_TX_CLIENT_RETRY_STRATEGY, "n-times");
    result.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_ATTEMPTS, 0);

    // Unset the hbase.client.retries.number and hbase.rpc.timeout so that program container
    // runs with default values for them from hbase-site/hbase-default.
    result.unset(Constants.HBase.CLIENT_RETRIES);
    result.unset(Constants.HBase.RPC_TIMEOUT);

    // Unset the runtime extension directory as the necessary extension jars should be shipped to the container
    // by the distributed ProgramRunner.
    result.unset(Constants.AppFabric.RUNTIME_EXT_DIR);

    // Set the CFG_LOCAL_DATA_DIR to a relative path as the data directory for the container should be relative to the
    // container directory
    result.set(Constants.CFG_LOCAL_DATA_DIR, "data");

    return result;
  }

  /**
   * Creates the {@link Configuration} to be used in the program container.
   */
  protected Configuration createContainerHConf(Program program, Configuration hConf) {
    Configuration result = new YarnConfiguration(hConf);
    // Unset the hbase.client.retries.number and hbase.rpc.timeout so that program container
    // runs with default values for them from hbase-site/hbase-default.
    result.unset(Constants.HBase.CLIENT_RETRIES);
    result.unset(Constants.HBase.RPC_TIMEOUT);

    return result;
  }

  /**
   * Adds extra jars as defined in {@link CConfiguration} to the container and container classpath.
   */
  private void addContainerJars(CConfiguration cConf,
                                Map<String, LocalizeResource> localizeResources, Collection<String> classpath) {
    // Create localize resources and update classpath for the container based on the "program.container.dist.jars"
    // configuration.
    List<String> containerExtraJars = new ArrayList<>();
    for (URI jarURI : CConfigurationUtil.getExtraJars(cConf)) {
      String scheme = jarURI.getScheme();
      LocalizeResource localizeResource = new LocalizeResource(jarURI, false);
      String localizedName = LocalizationUtils.getLocalizedName(jarURI);
      localizeResources.put(localizedName, localizeResource);
      classpath.add(localizedName);
      String jarPath = "file".equals(scheme) ? localizedName : jarURI.toString();
      containerExtraJars.add(jarPath);
    }

    // Set the "program.container.dist.jars" since files are already localized to the container.
    cConf.setStrings(Constants.AppFabric.PROGRAM_CONTAINER_DIST_JARS,
                     containerExtraJars.toArray(new String[containerExtraJars.size()]));
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

  /**
   * Adds the KMS secure store class to the {@link TwillPreparer} dependencies class.
   */
  private void addKMSSecureStoreClass(CConfiguration cConf, TwillPreparer preparer) {
    if (SecureStoreUtils.isKMSBacked(cConf) && SecureStoreUtils.isKMSCapable()) {
      preparer.withDependencies(SecureStoreUtils.getKMSSecureStore());
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
    try (InputStream is = resource.openStream()) {
      Files.copy(is, logbackFile.toPath());
    }
    return logbackFile.toURI();
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

  private File saveCConf(CConfiguration cConf, File file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      cConf.writeXml(writer);
    }
    return file;
  }

  private File saveHConf(Configuration conf, File file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      conf.writeXml(writer);
    }
    return file;
  }

  private File saveAppSpec(Program program, File file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
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

  /**
   * A {@link TwillPreparer} that exposes dependencies added.
   */
  private static final class ProgramTwillPreparer extends ForwardingTwillPreparer {

    private final TwillPreparer preparer;
    private final List<Class<?>> dependencies;

    private ProgramTwillPreparer(TwillPreparer preparer) {
      this.preparer = preparer;
      this.dependencies = new ArrayList<>();
    }

    @Override
    public TwillPreparer getDelegate() {
      return preparer;
    }

    @Override
    public TwillPreparer withDependencies(Class<?>...classes) {
      super.withDependencies(classes);
      dependencies.addAll(Arrays.asList(classes));
      return this;
    }

    @Override
    public TwillPreparer withDependencies(Iterable<Class<?>> classes) {
      super.withDependencies(classes);
      Iterables.addAll(dependencies, classes);
      return this;
    }

    List<Class<?>> getDependencies() {
      return dependencies;
    }
  }
}
