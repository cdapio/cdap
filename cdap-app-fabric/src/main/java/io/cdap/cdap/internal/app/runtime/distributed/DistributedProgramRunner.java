/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.util.ContextInitializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramControllerCreator;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.common.app.MainClassLoader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.CConfigurationUtil;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.CombineClassLoader;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.logging.LoggerLogHandler;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.twill.TwillAppLifecycleEventHandler;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtilFactory;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.program.LauncherUtils;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.LocalizationUtils;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.master.spi.twill.SecretDisk;
import io.cdap.cdap.master.spi.twill.SecureTwillPreparer;
import io.cdap.cdap.master.spi.twill.SecurityContext;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.store.SecureStoreUtils;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tephra.TxConstants;
import org.apache.twill.api.Configs;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the base framework for starting {@link Program} in the cluster.
 */
public abstract class DistributedProgramRunner implements ProgramRunner, ProgramControllerCreator {

  public static final String CDAP_CONF_FILE_NAME = "cConf.xml";
  public static final String HADOOP_CONF_FILE_NAME = "hConf.xml";
  public static final String APP_SPEC_FILE_NAME = "appSpec.json";
  public static final String PROGRAM_OPTIONS_FILE_NAME = "program.options.json";
  public static final String LOGBACK_FILE_NAME = "logback.xml";
  public static final String TETHER_CONF_FILE_NAME = "cdap-tether.xml";

  private static final Logger LOG = LoggerFactory.getLogger(DistributedProgramRunner.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
          new GsonBuilder())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
      .create();

  protected final CConfiguration cConf;
  protected final Configuration hConf;
  protected final ClusterMode clusterMode;
  // Set only if ProgramRunner is not running remotely
  protected NamespaceQueryAdmin namespaceQueryAdmin;
  private final TwillRunner twillRunner;
  private final Impersonator impersonator;
  private final LocationFactory locationFactory;

  protected DistributedProgramRunner(CConfiguration cConf, Configuration hConf,
      Impersonator impersonator,
      ClusterMode clusterMode, TwillRunner twillRunner,
      LocationFactory locationFactory) {
    this.twillRunner = twillRunner;
    this.hConf = hConf;
    this.cConf = cConf;
    this.impersonator = impersonator;
    this.clusterMode = clusterMode;
    this.locationFactory = locationFactory;
  }

  /**
   * Validates the options for the program. Subclasses can override this to also validate the
   * options for their sub-programs.
   */
  protected void validateOptions(Program program, ProgramOptions options) {
    // this will throw an exception if the custom tx timeout is invalid
    SystemArguments.validateTransactionTimeout(options.getUserArguments().asMap(), cConf);
  }

  /**
   * Provides the configuration for launching an program container.
   *
   * @param launchConfig the {@link ProgramLaunchConfig} to setup
   * @param program the program to launch
   * @param options the program options
   * @param cConf the configuration for this launch
   * @param hConf the hadoop configuration for this launch
   * @param tempDir a temporary directory for creating temp file. The content will be cleanup
   *     automatically once the program is launch.
   */
  protected abstract void setupLaunchConfig(ProgramLaunchConfig launchConfig, Program program,
      ProgramOptions options,
      CConfiguration cConf, Configuration hConf, File tempDir) throws IOException;

  @Override
  public final ProgramController run(final Program program, ProgramOptions oldOptions) {
    validateOptions(program, oldOptions);

    CConfiguration cConf = CConfiguration.copy(this.cConf);
    // Reload config for log extension jar update (CDAP-15091)
    cConf.reloadConfiguration();

    File tempDir = DirUtils.createTempDir(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
        cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
    try {
      // For runs from a tethered instance, load additional resources
      if (oldOptions.getArguments().hasOption(ProgramOptionConstants.PEER_NAME)) {
        loadAdditionalResources(oldOptions.getArguments(), cConf, tempDir);
      }
      ProgramLaunchConfig launchConfig = new ProgramLaunchConfig();
      if (clusterMode == ClusterMode.ISOLATED) {
        // For isolated mode, the hadoop classes comes from the hadoop classpath in the target cluster directly
        launchConfig.addExtraClasspath(Collections.singletonList("$HADOOP_CLASSPATH"));
      }
      setupLaunchConfig(launchConfig, program, oldOptions, cConf, hConf, tempDir);

      // Add extra localize resources needed by the program runner
      final Map<String, LocalizeResource> localizeResources = new HashMap<>(
          launchConfig.getExtraResources());
      final List<String> additionalClassPaths = new ArrayList<>();
      addContainerJars(cConf, localizeResources, additionalClassPaths);
      addAdditionalLogAppenderJars(cConf, tempDir, localizeResources,
          SystemArguments.getProfileProvisioner(oldOptions.getArguments().asMap()));

      prepareHBaseDDLExecutorResources(tempDir, cConf, localizeResources);

      localizeConfigs(createContainerCConf(cConf), createContainerHConf(this.hConf), tempDir,
          localizeResources);

      // Localize the app spec
      localizeResources.put(APP_SPEC_FILE_NAME,
          new LocalizeResource(saveJsonFile(program.getApplicationSpecification(),
              ApplicationSpecification.class,
              File.createTempFile("appSpec", ".json", tempDir))));

      URI logbackUri = getLogBackURI(program, oldOptions.getArguments(), tempDir);
      if (logbackUri != null) {
        // Localize the logback xml
        localizeResources.put(LOGBACK_FILE_NAME, new LocalizeResource(logbackUri, false));
      }

      // For runs from a tethered instance, copy over peer runtime token if it exists.
      if (oldOptions.getArguments().hasOption(ProgramOptionConstants.PEER_NAME)) {
        copyRuntimeToken(locationFactory, oldOptions.getArguments(), tempDir, localizeResources);
      }

      // Localize the program jar
      Location programJarLocation = program.getJarLocation();
      localizeResources.put(programJarLocation.getName(),
          new LocalizeResource(programJarLocation.toURI(), false));

      // Update the ProgramOptions to carry program and runtime information necessary to reconstruct the program
      // and runs it in the remote container
      Map<String, String> extraSystemArgs = getExtraSystemArgs(launchConfig, program, oldOptions);
      ProgramOptions options = updateProgramOptions(oldOptions, localizeResources,
          DirUtils.createTempDir(tempDir), extraSystemArgs);
      ProgramRunId programRunId = program.getId().run(ProgramRunners.getRunId(options));

      // Localize the serialized program options
      localizeResources.put(PROGRAM_OPTIONS_FILE_NAME,
          new LocalizeResource(saveJsonFile(
              options, ProgramOptions.class, new File(tempDir, PROGRAM_OPTIONS_FILE_NAME))));

      Callable<ProgramController> callable = () -> {
        Map<String, RunnableDefinition> twillRunnables = launchConfig.getRunnables();

        ProgramTwillApplication twillApplication = new ProgramTwillApplication(
            programRunId, options, twillRunnables, launchConfig.getLaunchOrder(),
            localizeResources, createEventHandler(cConf));

        TwillPreparer twillPreparer = twillRunner.prepare(twillApplication);

        // Also extra files to the resources jar so that the TwillAppLifecycleEventHandler,
        // which runs in the AM container, can get them.
        // This can be removed when TWILL-246 is fixed.
        // Only program running in Hadoop will be using EventHandler
        twillPreparer.withResources(localizeResources.get(CDAP_CONF_FILE_NAME).getURI(),
            localizeResources.get(HADOOP_CONF_FILE_NAME).getURI(),
            localizeResources.get(PROGRAM_OPTIONS_FILE_NAME).getURI());
        if (logbackUri != null) {
          twillPreparer.withResources(logbackUri);
        }


        // Setup log level
        Map<String, String> userArgs = options.getUserArguments().asMap();
        twillPreparer.setLogLevels(transformLogLevels(SystemArguments.getLogLevels(userArgs)));

        // Set the configuration for the twill application
        setTwillConfigs(twillPreparer, program, options, twillRunnables);

        if (options.isDebug()) {
          twillPreparer.enableDebugging();
        }

        logProgramStart(program, options);

        // Add scheduler queue name if defined
        setSchedulerQueue(twillPreparer, program, options);

        // Set JVM options based on configuration
        setJvmOpts(twillPreparer, options, logbackUri, twillRunnables);

        addLogHandler(twillPreparer, cConf);

        setEnv(twillPreparer, launchConfig);

        // Add dependencies
        Set<Class<?>> extraDependencies = addExtraDependencies(cConf,
            new HashSet<>(launchConfig.getExtraDependencies()));
        twillPreparer.withDependencies(extraDependencies);
        setClassPaths(twillPreparer, launchConfig, additionalClassPaths);
        twillPreparer
            .withBundlerClassAcceptor(launchConfig.getClassAcceptor())
            .withApplicationArguments(PROGRAM_OPTIONS_FILE_NAME)
            // Use the MainClassLoader for class rewriting
            .setClassLoader(MainClassLoader.class.getName());

        TwillController twillController;
        // Change the context classloader to the combine classloader of this ProgramRunner and
        // all the classloaders of the dependencies classes so that Twill can trace classes.
        ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(new CombineClassLoader(
            DistributedProgramRunner.this.getClass().getClassLoader(),
            extraDependencies.stream().map(Class::getClassLoader)::iterator));
        try {
          twillController = twillPreparer.start(
              cConf.getLong(Constants.AppFabric.PROGRAM_MAX_START_SECONDS),
              TimeUnit.SECONDS);

          /**
           * Block on the twill controller until it is in running state or terminated (due to failure).
           * If TWILL_CONTROLLER_START_SECONDS is set to zero, it means that we do not want to wait for twill
           * controller to go into running or terminated state. The reason is that monitoring will be happening
           * somewhere else.
           */
          if (cConf.getLong(Constants.AppFabric.TWILL_CONTROLLER_START_SECONDS) > 0) {
            CountDownLatch latch = new CountDownLatch(1);
            twillController.onRunning(latch::countDown, Threads.SAME_THREAD_EXECUTOR);
            twillController.onTerminated(latch::countDown, Threads.SAME_THREAD_EXECUTOR);
            latch.await(cConf.getLong(Constants.AppFabric.TWILL_CONTROLLER_START_SECONDS),
                TimeUnit.SECONDS);
          }
        } finally {
          ClassLoaders.setContextClassLoader(oldClassLoader);
        }

        return createProgramController(programRunId,
            addCleanupListener(twillController, program, tempDir));
      };

      return impersonator.doAs(programRunId, callable);

    } catch (Exception e) {
      deleteDirectory(tempDir);
      throw Throwables.propagate(e);
    }
  }

  @VisibleForTesting
  void setSchedulerQueue(TwillPreparer twillPreparer, Program program, ProgramOptions options) {
    String schedulerQueueName = options.getArguments()
        .getOption(Constants.AppFabric.APP_SCHEDULER_QUEUE);
    if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
      LOG.info("Setting scheduler queue for app {} as {}", program.getId(), schedulerQueueName);
      twillPreparer.setSchedulerQueue(schedulerQueueName);
    }
  }

  @VisibleForTesting
  Map<String, String> getExtraSystemArgs(ProgramLaunchConfig launchConfig,
      Program program, ProgramOptions options) {
    Map<String, String> extraSystemArgs = new HashMap<>(launchConfig.getExtraSystemArguments());
    String programJarName = program.getJarLocation().getName();
    if (options.getArguments().hasOption(ProgramOptionConstants.PROGRAM_JAR_HASH)) {
      // if hash value for program.jar has been provided, we append it to filename.
      String programJarHash = options.getArguments()
          .getOption(ProgramOptionConstants.PROGRAM_JAR_HASH);
      programJarName = programJarName.replace(".jar",
          String.format("_%s%s", programJarHash, ".jar"));

      Set<String> cacheableFiles = new HashSet<>();
      if (options.getArguments().hasOption(ProgramOptionConstants.CACHEABLE_FILES)) {
        cacheableFiles =
            new HashSet<>(GSON.fromJson(
                options.getArguments().getOption(ProgramOptionConstants.CACHEABLE_FILES),
                new TypeToken<Set<String>>() {
                }.getType()));
      }
      cacheableFiles.add(programJarName);
      extraSystemArgs.put(ProgramOptionConstants.CACHEABLE_FILES, GSON.toJson(cacheableFiles));
    }
    extraSystemArgs.put(ProgramOptionConstants.PROGRAM_JAR, programJarName);
    extraSystemArgs.put(ProgramOptionConstants.HADOOP_CONF_FILE, HADOOP_CONF_FILE_NAME);
    extraSystemArgs.put(ProgramOptionConstants.CDAP_CONF_FILE, CDAP_CONF_FILE_NAME);
    extraSystemArgs.put(ProgramOptionConstants.APP_SPEC_FILE, APP_SPEC_FILE_NAME);
    return extraSystemArgs;
  }

  @VisibleForTesting
  void setClassPaths(TwillPreparer twillPreparer, ProgramLaunchConfig launchConfig,
      List<String> additionalClassPaths) {
    // Add the additional classes to the classpath that comes from the container jar setting
    twillPreparer.withClassPaths(additionalClassPaths);

    twillPreparer.withClassPaths(launchConfig.getExtraClasspath());

    // Add the YARN_APPLICATION_CLASSPATH so that yarn classpath are included in the twill container.
    // The Yarn app classpath goes last
    List<String> yarnAppClassPath = Arrays.asList(
        hConf.getTrimmedStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));
    twillPreparer
        .withApplicationClassPaths(yarnAppClassPath)
        .withClassPaths(yarnAppClassPath);
  }

  @VisibleForTesting
  void setEnv(TwillPreparer twillPreparer, ProgramLaunchConfig launchConfig) {
    // Setup the environment for the container logback.xml
    twillPreparer.withEnv(
        Collections.singletonMap("CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR));
    twillPreparer.withEnv(launchConfig.getExtraEnv());
  }

  @VisibleForTesting
  void setTwillConfigs(TwillPreparer twillPreparer, Program program, ProgramOptions options,
      Map<String, RunnableDefinition> twillRunnables) {
    Map<String, String> twillConfigs = new HashMap<>();
    if (DistributedProgramRunner.this instanceof LongRunningDistributedProgramRunner) {
      twillConfigs.put(Configs.Keys.YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL,
          cConf.get(Constants.AppFabric.YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL));
    } else {
      // For non long running program type, set the max attempts to 1 to avoid YARN retry.
      // If the AM container dies, the program execution will be marked as failure.
      // Note that this setting is only applicable to the Twill YARN application
      // (e.g. workflow, Spark client, MR client, etc), but not to the actual Spark / MR job.
      twillConfigs.put(Configs.Keys.YARN_MAX_APP_ATTEMPTS, Integer.toString(1));
    }

    // Add twill configurations coming from the runtime arguments
    twillConfigs.putAll(SystemArguments.getNamespaceConfigs(options.getArguments().asMap()));
    twillConfigs.putAll(SystemArguments.getTwillApplicationConfigs(options.getUserArguments().asMap()));
    twillConfigs.put(ProgramOptionConstants.RUNTIME_NAMESPACE, program.getNamespaceId());
    twillPreparer.withConfiguration(twillConfigs);

    // Setup per runnable configurations
    for (Map.Entry<String, RunnableDefinition> entry : twillRunnables.entrySet()) {
      String runnable = entry.getKey();
      RunnableDefinition runnableDefinition = entry.getValue();
      if (runnableDefinition.getMaxRetries() != null) {
        twillPreparer.withMaxRetries(runnable, runnableDefinition.getMaxRetries());
      }
      twillPreparer.setLogLevels(runnable,
          transformLogLevels(runnableDefinition.getLogLevels()));
      twillPreparer.withConfiguration(runnable, runnableDefinition.getTwillRunnableConfigs());

      // Add cdap-security.xml if using secrets, and set the runnable identity.
      if (twillPreparer instanceof SecureTwillPreparer) {
        String twillSystemIdentity = cConf.get(Constants.Twill.Security.IDENTITY_SYSTEM);
        if (twillSystemIdentity != null) {
          SecurityContext securityContext = new SecurityContext.Builder()
              .withIdentity(twillSystemIdentity).build();
          twillPreparer = ((SecureTwillPreparer) twillPreparer).withSecurityContext(runnable,
              securityContext);
        }
        String securityName = cConf.get(Constants.Twill.Security.MASTER_SECRET_DISK_NAME);
        String securityPath = cConf.get(Constants.Twill.Security.MASTER_SECRET_DISK_PATH);
        twillPreparer = ((SecureTwillPreparer) twillPreparer)
            .withSecretDisk(runnable, new SecretDisk(securityName, securityPath));
      }
    }
  }

  @VisibleForTesting
  void setJvmOpts(TwillPreparer twillPreparer, ProgramOptions options,
      @Nullable URI logbackUri, Map<String, RunnableDefinition> twillRunnables) {
    String jvmOpts = cConf.get(Constants.AppFabric.PROGRAM_JVM_OPTS);
    String runtimeJvmOpts = options.getUserArguments().getOption(SystemArguments.JVM_OPTS);
    if (!Strings.isNullOrEmpty(runtimeJvmOpts)) {
      if (!Strings.isNullOrEmpty(jvmOpts)) {
        jvmOpts = jvmOpts + " " + runtimeJvmOpts;
      } else {
        jvmOpts = runtimeJvmOpts;
      }
    }

    if (!Strings.isNullOrEmpty(jvmOpts)) {
      twillPreparer.addJVMOptions(jvmOpts);
    }

    // Overwrite JVM options if specified
    Set<String> runnables = new HashSet<>(twillRunnables.keySet());
    LauncherUtils.overrideJVMOpts(cConf, twillPreparer, runnables);

    if (logbackUri != null) {
      // AM has the logback.xml file under resources.jar/resources/logback.xml
      twillPreparer.addJVMOptions("-D" + ContextInitializer.CONFIG_FILE_PROPERTY
          + "=resources.jar/resources/" + LOGBACK_FILE_NAME);
      // Set the system property to be used by the logback xml
      twillPreparer.addJVMOptions(
          "-DCDAP_LOG_DIR=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);

      // Runnable has the logback.xml just in the home directory
      for (String runnableName : runnables) {
        twillPreparer.setJVMOptions(runnableName,
            "-D" + ContextInitializer.CONFIG_FILE_PROPERTY + "=" + LOGBACK_FILE_NAME);
      }
    }
  }

  /**
   * For programs initiated by a tethered peer, load additional resources that were saved based by
   * runId
   */
  private void loadAdditionalResources(Arguments args, CConfiguration cConf, File tempDir)
      throws IOException {
    Location location = locationFactory.create(
        args.getOption(ProgramOptionConstants.PROGRAM_RESOURCE_URI));
    // add additional cConf entries
    File tetherCConfFile = File.createTempFile("cdap-tether", ".xml", tempDir);
    try (InputStream is = location.append(TETHER_CONF_FILE_NAME).getInputStream()) {
      Files.copy(is, tetherCConfFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      LOG.debug("Copied {} to {}", TETHER_CONF_FILE_NAME, tetherCConfFile);
    }
    cConf.addResource(tetherCConfFile.toURI().toURL());
  }

  /**
   * Adds a {@link LocalizeResource} if extra log appender is being configured.
   */
  private void addAdditionalLogAppenderJars(CConfiguration cConf, File tempDir,
      Map<String, LocalizeResource> localizeResources,
      String provisioner) throws IOException {
    String provider = cConf.get(Constants.Logging.LOG_APPENDER_PROVIDER);
    if (Strings.isNullOrEmpty(provider)) {
      return;
    }
    // If the log appender provider doesn't support the provisioner for the program execution, unset the config.
    Collection<String> provisioners = cConf.getTrimmedStringCollection(
        Constants.Logging.LOG_APPENDER_PROVISIONERS);
    if (!provisioners.contains(provisioner)) {
      cConf.unset(Constants.Logging.LOG_APPENDER_PROVIDER);
      return;
    }

    LOG.debug("Configure log appender provider to {}", provider);

    String localizedDir = "log-appender-ext";
    File logAppenderArchive = new File(tempDir, localizedDir + ".zip");
    try (ZipOutputStream zipOut = new ZipOutputStream(
        Files.newOutputStream(logAppenderArchive.toPath()))) {
      BundleJarUtil.addToArchive(
          new File(cConf.get(Constants.Logging.LOG_APPENDER_EXT_DIR), provider), true, zipOut);
    }
    // set extensions dir to point to localized appender directory - appender/<log-appender-provider>
    localizeResources.put(localizedDir, new LocalizeResource(logAppenderArchive, true));
    cConf.set(Constants.Logging.LOG_APPENDER_EXT_DIR, localizedDir);
  }

  /**
   * Creates the {@link CConfiguration} to be used in the program container.
   */
  private CConfiguration createContainerCConf(CConfiguration cConf) {
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

    // Setup the configurations for isolated mode
    if (clusterMode == ClusterMode.ISOLATED) {
      // Disable logs collection through twill
      result.set(Constants.COLLECT_APP_CONTAINER_LOG_LEVEL, "OFF");

      // Disable implicit transaction
      result.set(Constants.AppFabric.PROGRAM_TRANSACTION_CONTROL,
          TransactionControl.EXPLICIT.name());

      // Disable transaction support
      result.setBoolean(Constants.Transaction.TX_ENABLED, false);

      // Always use NoSQL as storage
      result.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION,
          Constants.Dataset.DATA_STORAGE_NOSQL);

      // The following services will be running in the edge node host.
      // Set the bind addresses for all of them to "${master.services.bind.address}"
      // Which the value of `"master.services.bind.address" will be set in the DefaultRuntimeJob when it
      // starts in the remote machine
      String masterBindAddrConf = "${" + Constants.Service.MASTER_SERVICES_BIND_ADDRESS + "}";
      result.set(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS, masterBindAddrConf);

      // Don't try and use the CDAP system certs for SSL
      result.unset(Constants.Security.SSL.INTERNAL_CERT_PATH);
    }

    return result;
  }

  /**
   * Creates the {@link Configuration} to be used in the program container.
   */
  private Configuration createContainerHConf(Configuration hConf) {
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
        containerExtraJars.toArray(new String[0]));
  }

  /**
   * Set up Logging Context so the Log is tagged correctly for the Program. Reset the context once
   * done.
   */
  private void logProgramStart(Program program, ProgramOptions options) {
    LoggingContext loggingContext =
        LoggingContextHelper.getLoggingContext(program.getNamespaceId(), program.getApplicationId(),
            program.getName(), program.getType(),
            ProgramRunners.getRunId(options).getId(),
            options.getArguments().asMap());
    Cancellable saveContextCancellable =
        LoggingContextAccessor.setLoggingContext(loggingContext);
    String userArguments = Joiner.on(", ").withKeyValueSeparator("=")
        .join(options.getUserArguments());

    LOG.info("Starting {} Program '{}' with Arguments [{}], with debugging {}",
        program.getType(), program.getName(), userArguments, options.isDebug());
    saveContextCancellable.cancel();
  }

  /**
   * Creates a new instance of {@link ProgramOptions} with artifact localization information and
   * with extra system arguments, while maintaining other fields of the given {@link
   * ProgramOptions}.
   *
   * @param options the original {@link ProgramOptions}.
   * @param localizeResources a {@link Map} of {@link LocalizeResource} to be localized to the
   *     remote container
   * @param tempDir a local temporary directory for creating files for artifact localization.
   * @param extraSystemArgs a set of extra system arguments to be added/updated
   * @return a new instance of {@link ProgramOptions}
   * @throws IOException if failed to create local copy of artifact files
   */
  private ProgramOptions updateProgramOptions(ProgramOptions options,
      Map<String, LocalizeResource> localizeResources,
      File tempDir, Map<String, String> extraSystemArgs) throws IOException {
    Arguments systemArgs = options.getArguments();

    Map<String, String> newSystemArgs = new HashMap<>(systemArgs.asMap());
    newSystemArgs.putAll(extraSystemArgs);

    String pluginDirFileName = ProgramRunners.PLUGIN_DIR;
    String pluginArchiveFileName = ProgramRunners.PLUGIN_ARCHIVE;
    if (systemArgs.hasOption(ProgramOptionConstants.PLUGIN_ARCHIVE)) {
      // If the archive already exists locally, we just need to re-localize it to remote containers
      File archiveFile = new File(systemArgs.getOption(ProgramOptionConstants.PLUGIN_ARCHIVE));
      // Localize plugins to two files, one expanded into a directory, one not.
      localizeResources.put(pluginDirFileName, new LocalizeResource(archiveFile, true));
      localizeResources.put(pluginArchiveFileName, new LocalizeResource(archiveFile, false));
    } else if (systemArgs.hasOption(ProgramOptionConstants.PLUGIN_DIR)) {
      // If there is a plugin directory,
      // then we need to create an archive and localize it to remote containers
      File archiveFile = ProgramRunners.createPluginArchive(options, tempDir);

      if (systemArgs.hasOption(ProgramOptionConstants.PLUGIN_DIR_HASH)) {
        // if hash value for plugins has been provided, we append it to filename.
        String pluginDirHash = systemArgs.getOption(ProgramOptionConstants.PLUGIN_DIR_HASH);
        newSystemArgs.remove(ProgramOptionConstants.PLUGIN_DIR_HASH);
        pluginDirFileName = String.format("%s_%s", ProgramRunners.PLUGIN_DIR, pluginDirHash);
        pluginArchiveFileName = ProgramRunners.PLUGIN_ARCHIVE.replace(".jar",
            String.format("_%s%s", pluginDirHash, ".jar"));

        Set<String> cacheableFiles = new HashSet<>();
        if (newSystemArgs.containsKey(ProgramOptionConstants.CACHEABLE_FILES)) {
          cacheableFiles = new HashSet<>(
              GSON.fromJson(newSystemArgs.get(ProgramOptionConstants.CACHEABLE_FILES),
                  new TypeToken<Set<String>>() {
                  }.getType()));
        }
        cacheableFiles.add(pluginDirFileName);
        cacheableFiles.add(pluginArchiveFileName);
        newSystemArgs.put(ProgramOptionConstants.CACHEABLE_FILES, GSON.toJson(cacheableFiles));
      }
      if (systemArgs.hasOption(ProgramOptionConstants.PROGRAM_JAR_HASH)) {
        newSystemArgs.remove(ProgramOptionConstants.PROGRAM_JAR_HASH);
      }

      // Localize plugins to two files, one expanded into a directory, one not.
      localizeResources.put(pluginDirFileName, new LocalizeResource(archiveFile, true));
      localizeResources.put(pluginArchiveFileName, new LocalizeResource(archiveFile, false));
    }

    // Add/rename the entries in the system arguments
    if (localizeResources.containsKey(pluginDirFileName)) {
      newSystemArgs.put(ProgramOptionConstants.PLUGIN_DIR, pluginDirFileName);
    }
    if (localizeResources.containsKey(pluginArchiveFileName)) {
      newSystemArgs.put(ProgramOptionConstants.PLUGIN_ARCHIVE, pluginArchiveFileName);
    }
    newSystemArgs.put(SystemArguments.PROFILE_PROPERTIES_PREFIX
            + Constants.AppFabric.ARTIFACTS_COMPUTE_HASH_TIME_BUCKET_DAYS,
        String.valueOf(cConf.getInt(Constants.AppFabric.ARTIFACTS_COMPUTE_HASH_TIME_BUCKET_DAYS)));

    return new SimpleProgramOptions(options.getProgramId(), new BasicArguments(newSystemArgs),
        options.getUserArguments(), options.isDebug());
  }

  /**
   * Returns a {@link URI} for the logback.xml file to be localized to container and available in
   * the container classpath.
   */
  @Nullable
  private URI getLogBackURI(Program program, Arguments args, File tempDir) throws IOException {
    File logbackFile = new File(tempDir, LOGBACK_FILE_NAME);

    // For runs from a tethered instance, use given logback file
    if (args.hasOption(ProgramOptionConstants.PEER_NAME)) {
      Location location = locationFactory
          .create(args.getOption(ProgramOptionConstants.PROGRAM_RESOURCE_URI))
          .append(LOGBACK_FILE_NAME);

      try (InputStream is = location.getInputStream()) {
        Files.copy(is, logbackFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        LOG.debug("Copied {} to {}", LOGBACK_FILE_NAME, logbackFile);
      }
      return logbackFile.toURI();
    }

    // Find and copy the logback xml to a predefined file name.

    String configurationFile = System.getProperty("logback.configurationFile");
    if (configurationFile != null) {
      Files.copy(new File(configurationFile).toPath(), logbackFile.toPath(),
          StandardCopyOption.REPLACE_EXISTING);
      return logbackFile.toURI();
    }

    // List of possible locations of where the logback xml is.
    URL logbackURL = Stream.of(program.getClassLoader().getResource("logback.xml"),
            getClass().getClassLoader().getResource("logback-container.xml"),
            getClass().getClassLoader().getResource("logback.xml"))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);

    if (logbackURL == null) {
      return null;
    }

    try (OutputStream os = Files.newOutputStream(logbackFile.toPath())) {
      Resources.copy(logbackURL, os);
    }
    return logbackFile.toURI();
  }

  private Map<String, LogEntry.Level> transformLogLevels(Map<String, Level> logLevels) {
    return logLevels.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> convertLogLevel(e.getValue())));
  }

  /**
   * Converts a logback {@link Level} into twill {@link LogEntry.Level}.
   */
  private LogEntry.Level convertLogLevel(Level level) {
    // Twill LogEntry.Level doesn't have ALL and OFF, so map them to lowest and highest log level respectively
    if (level.equals(Level.ALL)) {
      return LogEntry.Level.TRACE;
    }
    if (level.equals(Level.OFF)) {
      return LogEntry.Level.FATAL;
    }
    return LogEntry.Level.valueOf(level.toString());
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

  private <T> File saveJsonFile(T obj, Class<T> type, File file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      GSON.toJson(obj, type, writer);
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
   * Adds a listener to the given TwillController to delete local temp files when the program has
   * started/terminated. The local temp files could be removed once the program is started, since
   * Twill would keep the files in HDFS and no long needs the local temp files once program is
   * started.
   *
   * @return The same TwillController instance.
   */
  private TwillController addCleanupListener(TwillController controller,
      final Program program, final File tempDir) {

    final AtomicBoolean deleted = new AtomicBoolean(false);
    Runnable cleanup = () -> {
      if (!deleted.compareAndSet(false, true)) {
        return;
      }
      LOG.debug("Cleanup tmp files for {}: {}", program.getId(), tempDir);
      deleteDirectory(tempDir);
    };
    controller.onRunning(cleanup, Threads.SAME_THREAD_EXECUTOR);
    controller.onTerminated(cleanup, Threads.SAME_THREAD_EXECUTOR);
    return controller;
  }

  /**
   * Prepares the {@link HBaseDDLExecutor} implementation for localization.
   */
  private void prepareHBaseDDLExecutorResources(File tempDir, CConfiguration cConf,
      Map<String, LocalizeResource> localizeResources) throws IOException {
    String ddlExecutorExtensionDir = cConf.get(Constants.HBaseDDLExecutor.EXTENSIONS_DIR);

    // Only support HBase when running on premise
    if (ddlExecutorExtensionDir == null || clusterMode != ClusterMode.ON_PREMISE) {
      // Nothing to localize
      return;
    }

    final File target = new File(tempDir, "hbaseddlext.jar");
    BundleJarUtil.createJar(new File(ddlExecutorExtensionDir), target);
    localizeResources.put(target.getName(), new LocalizeResource(target, true));
    cConf.set(Constants.HBaseDDLExecutor.EXTENSIONS_DIR, target.getName());
  }

  /**
   * Creates the {@link EventHandler} for handling the application events.
   */
  private EventHandler createEventHandler(CConfiguration cConf) {
    return new TwillAppLifecycleEventHandler(
        cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE),
        this instanceof LongRunningDistributedProgramRunner);
  }

  /**
   * Localizes the configuration files.
   *
   * @param cConf the CDAP configuration to localize
   * @param hConf the hadoop configuration to localize
   * @param tempDir a temporary directory for local file creation
   * @param localizeResources the {@link LocalizeResource} map to update
   */
  private void localizeConfigs(CConfiguration cConf, Configuration hConf, File tempDir,
      Map<String, LocalizeResource> localizeResources) throws IOException {
    File cConfFile = saveCConf(cConf, new File(tempDir, CDAP_CONF_FILE_NAME));
    localizeResources.put(CDAP_CONF_FILE_NAME, new LocalizeResource(cConfFile));

    // Save the configuration to files
    File hConfFile = saveHConf(hConf, new File(tempDir, HADOOP_CONF_FILE_NAME));
    localizeResources.put(HADOOP_CONF_FILE_NAME, new LocalizeResource(hConfFile));
  }

  /**
   * Adds a {@link LogHandler} to the {@link TwillPreparer} based on the configuration.
   */
  private void addLogHandler(TwillPreparer twillPreparer, CConfiguration cConf) {
    String confLevel = cConf.get(Constants.COLLECT_APP_CONTAINER_LOG_LEVEL).toUpperCase();
    if ("OFF".equals(confLevel)) {
      twillPreparer.withConfiguration(
          Collections.singletonMap(Configs.Keys.LOG_COLLECTION_ENABLED, "false"));
      return;
    }

    LogEntry.Level logLevel = LogEntry.Level.ERROR;
    try {
      logLevel = "ALL".equals(confLevel) ? LogEntry.Level.TRACE
          : LogEntry.Level.valueOf(confLevel.toUpperCase());
    } catch (Exception e) {
      LOG.warn("Invalid application container log level {}. Defaulting to ERROR.", confLevel);
    }
    twillPreparer.addLogHandler(new LoggerLogHandler(LOG, logLevel));
  }

  /**
   * Adds extra dependency classes based on the given configuration.
   */
  private Set<Class<?>> addExtraDependencies(CConfiguration cConf, Set<Class<?>> dependencies) {
    // Only support HBase and KMS when running on premise
    if (clusterMode == ClusterMode.ON_PREMISE) {
      try {
        dependencies.add(HBaseTableUtilFactory.getHBaseTableUtilClass(cConf));
      } catch (Exception e) {
        LOG.debug("No HBase dependencies to add.");
      }
      if (SecureStoreUtils.isKMSBacked(cConf) && SecureStoreUtils.isKMSCapable()) {
        dependencies.add(SecureStoreUtils.getKMSSecureStore());
      }
    }

    // When it is running in Hadoop, we need to add YarnClient to dependency since we will be submitting job to YARN.
    if (clusterMode == ClusterMode.ON_PREMISE || cConf.getBoolean(
        Constants.AppFabric.PROGRAM_REMOTE_RUNNER, false)) {
      dependencies.add(YarnClient.class);
    }

    return dependencies;
  }

  @VisibleForTesting
  static void copyRuntimeToken(LocationFactory locationFactory, Arguments args, File tempDir,
      Map<String, LocalizeResource> localizeResources) throws IOException {
    File runtimeTokenFile = File.createTempFile("runtime-tether", ".token", tempDir);
    Location location = locationFactory.create(
            URI.create(args.getOption(ProgramOptionConstants.PROGRAM_RESOURCE_URI)))
        .append(Constants.Security.Authentication.RUNTIME_TOKEN_FILE);
    if (!location.exists()) {
      return;
    }
    try (InputStream is = location.getInputStream()) {
      Files.copy(is, runtimeTokenFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      LOG.debug("Copied {} to {}", Constants.Security.Authentication.RUNTIME_TOKEN_FILE,
          runtimeTokenFile);
    }
    localizeResources.put(Constants.Security.Authentication.RUNTIME_TOKEN_FILE,
        new LocalizeResource(runtimeTokenFile.toURI(), false));
  }
}
