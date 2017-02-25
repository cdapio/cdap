/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.yarn;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.io.OutputSupplier;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import joptsimple.OptionSpec;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.Configs;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.Arguments;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultLocalFile;
import org.apache.twill.internal.DefaultRuntimeSpecification;
import org.apache.twill.internal.DefaultTwillSpecification;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.JvmOptions;
import org.apache.twill.internal.LogOnlyEventHandler;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.ProcessLauncher;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.appmaster.ApplicationMasterInfo;
import org.apache.twill.internal.appmaster.ApplicationMasterMain;
import org.apache.twill.internal.container.TwillContainerMain;
import org.apache.twill.internal.io.LocationCache;
import org.apache.twill.internal.json.ArgumentsCodec;
import org.apache.twill.internal.json.JvmOptionsCodec;
import org.apache.twill.internal.json.LocalFileCodec;
import org.apache.twill.internal.json.TwillRuntimeSpecificationAdapter;
import org.apache.twill.internal.utils.Dependencies;
import org.apache.twill.internal.utils.Paths;
import org.apache.twill.internal.utils.Resources;
import org.apache.twill.internal.yarn.YarnAppClient;
import org.apache.twill.internal.yarn.YarnApplicationReport;
import org.apache.twill.internal.yarn.YarnUtils;
import org.apache.twill.launcher.FindFreePort;
import org.apache.twill.launcher.TwillLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Implementation for {@link TwillPreparer} to prepare and launch distributed application on Hadoop YARN.
 */
final class YarnTwillPreparer implements TwillPreparer {

  private static final Logger LOG = LoggerFactory.getLogger(YarnTwillPreparer.class);
  private static final Function<Class<?>, String> CLASS_TO_NAME = new Function<Class<?>, String>() {
    @Override
    public String apply(Class<?> cls) {
      return cls.getName();
    }
  };

  private final YarnConfiguration yarnConfig;
  private final TwillSpecification twillSpec;
  private final YarnAppClient yarnAppClient;
  private final String zkConnectString;
  private final Location appLocation;
  private final YarnTwillControllerFactory controllerFactory;
  private final RunId runId;

  private final List<LogHandler> logHandlers = Lists.newArrayList();
  private final List<String> arguments = Lists.newArrayList();
  private final Set<Class<?>> dependencies = Sets.newIdentityHashSet();
  private final List<URI> resources = Lists.newArrayList();
  private final List<String> classPaths = Lists.newArrayList();
  private final ListMultimap<String, String> runnableArgs = ArrayListMultimap.create();
  private final Map<String, Map<String, String>> environments = Maps.newHashMap();
  private final List<String> applicationClassPaths = Lists.newArrayList();
  private final Credentials credentials;
  private final int reservedMemory;
  private final double minHeapRatio;
  private final File localStagingDir;
  private final Map<String, Map<String, String>> logLevels = Maps.newHashMap();
  private final LocationCache locationCache;
  private final Set<URL> twillClassPaths;
  private String schedulerQueue;
  private String extraOptions;
  private JvmOptions.DebugOptions debugOptions = JvmOptions.DebugOptions.NO_DEBUG;
  private ClassAcceptor classAcceptor;
  private final Map<String, Integer> maxRetries = Maps.newHashMap();

  YarnTwillPreparer(YarnConfiguration yarnConfig, TwillSpecification twillSpec, RunId runId,
                    YarnAppClient yarnAppClient, String zkConnectString, Location appLocation, Set<URL> twillClassPaths,
                    String extraOptions, LocationCache locationCache, YarnTwillControllerFactory controllerFactory) {
    this.yarnConfig = yarnConfig;
    this.twillSpec = twillSpec;
    this.runId = runId;
    this.yarnAppClient = yarnAppClient;
    this.zkConnectString = zkConnectString;
    this.appLocation = appLocation;
    this.controllerFactory = controllerFactory;
    this.credentials = createCredentials();
    this.reservedMemory = yarnConfig.getInt(Configs.Keys.JAVA_RESERVED_MEMORY_MB,
                                            Configs.Defaults.JAVA_RESERVED_MEMORY_MB);
    // doing this way to support hadoop-2.0 profile
    String minHeapRatioStr = yarnConfig.get(Configs.Keys.HEAP_RESERVED_MIN_RATIO);
    this.minHeapRatio = (minHeapRatioStr == null) ?
      Configs.Defaults.HEAP_RESERVED_MIN_RATIO : Double.parseDouble(minHeapRatioStr);
    this.localStagingDir = new File(yarnConfig.get(Configs.Keys.LOCAL_STAGING_DIRECTORY,
                                                   Configs.Defaults.LOCAL_STAGING_DIRECTORY));
    this.extraOptions = extraOptions;
    this.classAcceptor = new ClassAcceptor();
    this.locationCache = locationCache;
    this.twillClassPaths = twillClassPaths;

    // By default, the root logger is having INFO log level
    setLogLevel(LogEntry.Level.INFO);
  }

  private void confirmRunnableName(String runnableName) {
    Preconditions.checkNotNull(runnableName);
    Preconditions.checkArgument(twillSpec.getRunnables().containsKey(runnableName),
                                "Runnable %s is not defined in the application.", runnableName);
  }

  @Override
  public TwillPreparer addLogHandler(LogHandler handler) {
    logHandlers.add(handler);
    return this;
  }

  @Override
  public TwillPreparer setUser(String user) {
    return this;
  }

  @Override
  public TwillPreparer setSchedulerQueue(String name) {
    this.schedulerQueue = name;
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String options) {
    this.extraOptions = options;
    return this;
  }

  @Override
  public TwillPreparer addJVMOptions(String options) {
    this.extraOptions = extraOptions == null ? options : extraOptions + " " + options;
    return this;
  }

  @Override
  public TwillPreparer enableDebugging(String... runnables) {
    return enableDebugging(false, runnables);
  }

  @Override
  public TwillPreparer enableDebugging(boolean doSuspend, String... runnables) {
    this.debugOptions = new JvmOptions.DebugOptions(true, doSuspend, ImmutableSet.copyOf(runnables));
    return this;
  }

  @Override
  public TwillPreparer withApplicationArguments(String... args) {
    return withApplicationArguments(ImmutableList.copyOf(args));
  }

  @Override
  public TwillPreparer withApplicationArguments(Iterable<String> args) {
    Iterables.addAll(arguments, args);
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    return withArguments(runnableName, ImmutableList.copyOf(args));
  }

  @Override
  public TwillPreparer withArguments(String runnableName, Iterable<String> args) {
    confirmRunnableName(runnableName);
    runnableArgs.putAll(runnableName, args);
    return this;
  }

  @Override
  public TwillPreparer withDependencies(Class<?>... classes) {
    return withDependencies(ImmutableList.copyOf(classes));
  }

  @Override
  public TwillPreparer withDependencies(Iterable<Class<?>> classes) {
    Iterables.addAll(dependencies, classes);
    return this;
  }

  @Override
  public TwillPreparer withResources(URI... resources) {
    return withResources(ImmutableList.copyOf(resources));
  }

  @Override
  public TwillPreparer withResources(Iterable<URI> resources) {
    Iterables.addAll(this.resources, resources);
    return this;
  }

  @Override
  public TwillPreparer withClassPaths(String... classPaths) {
    return withClassPaths(ImmutableList.copyOf(classPaths));
  }

  @Override
  public TwillPreparer withClassPaths(Iterable<String> classPaths) {
    Iterables.addAll(this.classPaths, classPaths);
    return this;
  }

  @Override
  public TwillPreparer withEnv(Map<String, String> env) {
    // Add the given environments to all runnables
    for (String runnableName : twillSpec.getRunnables().keySet()) {
      setEnv(runnableName, env, false);
    }
    return this;
  }

  @Override
  public TwillPreparer withEnv(String runnableName, Map<String, String> env) {
    confirmRunnableName(runnableName);
    setEnv(runnableName, env, true);
    return this;
  }

  @Override
  public TwillPreparer withApplicationClassPaths(String... classPaths) {
    return withApplicationClassPaths(ImmutableList.copyOf(classPaths));
  }

  @Override
  public TwillPreparer withApplicationClassPaths(Iterable<String> classPaths) {
    Iterables.addAll(this.applicationClassPaths, classPaths);
    return this;
  }

  @Override
  public TwillPreparer withBundlerClassAcceptor(ClassAcceptor classAcceptor) {
    this.classAcceptor = classAcceptor;
    return this;
  }

  @Override
  public TwillPreparer withMaxRetries(String runnableName, int maxRetries) {
    confirmRunnableName(runnableName);
    this.maxRetries.put(runnableName, maxRetries);
    return this;
  }

  @Override
  public TwillPreparer addSecureStore(SecureStore secureStore) {
    Object store = secureStore.getStore();
    Preconditions.checkArgument(store instanceof Credentials, "Only Hadoop Credentials is supported.");
    this.credentials.mergeAll((Credentials) store);
    return this;
  }

  @Override
  public TwillPreparer setLogLevel(LogEntry.Level logLevel) {
    return setLogLevels(ImmutableMap.of(Logger.ROOT_LOGGER_NAME, logLevel));
  }

  @Override
  public TwillPreparer setLogLevels(Map<String, LogEntry.Level> logLevels) {
    Preconditions.checkNotNull(logLevels);
    for (String runnableName : twillSpec.getRunnables().keySet()) {
      saveLogLevels(runnableName, logLevels);
    }
    return this;
  }

  @Override
  public TwillPreparer setLogLevels(String runnableName, Map<String, LogEntry.Level> runnableLogLevels) {
    confirmRunnableName(runnableName);
    Preconditions.checkNotNull(runnableLogLevels);
    Preconditions.checkArgument(!(logLevels.containsKey(Logger.ROOT_LOGGER_NAME)
      && logLevels.get(Logger.ROOT_LOGGER_NAME) == null));
    saveLogLevels(runnableName, runnableLogLevels);
    return this;
  }

  @Override
  public TwillController start() {
    return start(Constants.APPLICATION_MAX_START_SECONDS, TimeUnit.SECONDS);
  }

  @Override
  public TwillController start(long timeout, TimeUnit timeoutUnit) {
    try {
      final ProcessLauncher<ApplicationMasterInfo> launcher = yarnAppClient.createLauncher(twillSpec, schedulerQueue);
      final ApplicationMasterInfo appMasterInfo = launcher.getContainerInfo();
      Callable<ProcessController<YarnApplicationReport>> submitTask =
        new Callable<ProcessController<YarnApplicationReport>>() {
          @Override
          public ProcessController<YarnApplicationReport> call() throws Exception {

            // Local files needed by AM
            Map<String, LocalFile> localFiles = Maps.newHashMap();

            createLauncherJar(localFiles);
            createTwillJar(createBundler(classAcceptor), localFiles);
//            createApplicationJar(createApplicationJarBundler(classAcceptor), localFiles);
            // TODO: CDAP-8170. Because we copy classes from twill, hence when tracing twill dependencies,
            // CDAP classes gets included. When we create app, we can't exclude those twill dependencies
            // (which has CDAP in it), otherwise it will result in missing some CDAP jars.
            createApplicationJar(createBundler(classAcceptor), localFiles);
            createResourcesJar(createBundler(classAcceptor), localFiles);

            Path runtimeConfigDir = Files.createTempDirectory(localStagingDir.toPath(),
                                                              Constants.Files.RUNTIME_CONFIG_JAR);
            try {
              saveSpecification(twillSpec, runtimeConfigDir.resolve(Constants.Files.TWILL_SPEC));
              saveLogback(runtimeConfigDir.resolve(Constants.Files.LOGBACK_TEMPLATE));
              saveClassPaths(runtimeConfigDir);
              saveJvmOptions(runtimeConfigDir.resolve(Constants.Files.JVM_OPTIONS));
              saveArguments(new Arguments(arguments, runnableArgs),
                            runtimeConfigDir.resolve(Constants.Files.ARGUMENTS));
              saveEnvironments(runtimeConfigDir.resolve(Constants.Files.ENVIRONMENTS));
              createRuntimeConfigJar(runtimeConfigDir, localFiles);
            } finally {
              Paths.deleteRecursively(runtimeConfigDir);
            }

            createLocalizeFilesJson(localFiles);

            LOG.debug("Submit AM container spec: {}", appMasterInfo);
            // java -Djava.io.tmpdir=tmp -cp launcher.jar:$HADOOP_CONF_DIR -XmxMemory
            //     org.apache.twill.internal.TwillLauncher
            //     appMaster.jar
            //     org.apache.twill.internal.appmaster.ApplicationMasterMain
            //     false

            int memory = Resources.computeMaxHeapSize(appMasterInfo.getMemoryMB(),
                                                      Constants.APP_MASTER_RESERVED_MEMORY_MB,
                                                      Constants.HEAP_MIN_RATIO);
            return launcher.prepareLaunch(ImmutableMap.<String, String>of(), localFiles.values(), credentials)
              .addCommand(
                "$JAVA_HOME/bin/java",
                "-Djava.io.tmpdir=tmp",
                "-Dyarn.appId=$" + EnvKeys.YARN_APP_ID_STR,
                "-Dtwill.app=$" + Constants.TWILL_APP_NAME,
                "-cp", Constants.Files.LAUNCHER_JAR + ":$HADOOP_CONF_DIR",
                "-Xmx" + memory + "m",
                extraOptions == null ? "" : extraOptions,
                TwillLauncher.class.getName(),
                ApplicationMasterMain.class.getName(),
                Boolean.FALSE.toString())
              .launch();
          }
        };

      YarnTwillController controller = controllerFactory.create(runId, logHandlers, submitTask, timeout, timeoutUnit);
      controller.start();
      return controller;
    } catch (Exception e) {
      LOG.error("Failed to submit application {}", twillSpec.getName(), e);
      throw Throwables.propagate(e);
    }
  }

  private void setEnv(String runnableName, Map<String, String> env, boolean overwrite) {
    Map<String, String> environment = environments.get(runnableName);
    if (environment == null) {
      environment = new LinkedHashMap<>(env);
      environments.put(runnableName, environment);
      return;
    }

    for (Map.Entry<String, String> entry : env.entrySet()) {
      if (overwrite || !environment.containsKey(entry.getKey())) {
        environment.put(entry.getKey(), entry.getValue());
      }
    }
  }

  private void saveLogLevels(String runnableName, Map<String, LogEntry.Level> logLevels) {
    Map<String, String> newLevels = new HashMap<>();
    for (Map.Entry<String, LogEntry.Level> entry : logLevels.entrySet()) {
      Preconditions.checkArgument(entry.getValue() != null, "Log level cannot be null for logger {}", entry.getKey());
      newLevels.put(entry.getKey(), entry.getValue().name());
    }
    this.logLevels.put(runnableName, newLevels);
  }

  private Credentials createCredentials() {
    Credentials credentials = new Credentials();

    try {
      credentials.addAll(UserGroupInformation.getCurrentUser().getCredentials());

      List<Token<?>> tokens = YarnUtils.addDelegationTokens(yarnConfig, appLocation.getLocationFactory(), credentials);
      if (LOG.isDebugEnabled()) {
        for (Token<?> token : tokens) {
          LOG.debug("Delegation token acquired for {}, {}", appLocation, token);
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to check for secure login type. Not gathering any delegation token.", e);
    }
    return credentials;
  }

  private LocalFile createLocalFile(String name, Location location) throws IOException {
    return createLocalFile(name, location, false);
  }

  private LocalFile createLocalFile(String name, Location location, boolean archive) throws IOException {
    return new DefaultLocalFile(name, location.toURI(), location.lastModified(), location.length(), archive, null);
  }

  private void createTwillJar(final ApplicationBundler bundler, Map<String, LocalFile> localFiles) throws IOException {
    LOG.debug("Create and copy {}", Constants.Files.TWILL_JAR);
    Location location = locationCache.get(Constants.Files.TWILL_JAR, new LocationCache.Loader() {
      @Override
      public void load(String name, Location targetLocation) throws IOException {
        // Stuck in the yarnAppClient class to make bundler being able to pickup the right yarn-client version
        bundler.createBundle(targetLocation, ApplicationMasterMain.class,
                             yarnAppClient.getClass(), TwillContainerMain.class, OptionSpec.class);
      }
    });

    LOG.debug("Done {}", Constants.Files.TWILL_JAR);
    localFiles.put(Constants.Files.TWILL_JAR, createLocalFile(Constants.Files.TWILL_JAR, location, true));
  }

  private void createApplicationJar(final ApplicationBundler bundler,
                                    Map<String, LocalFile> localFiles) throws IOException {
    try {
      final Set<Class<?>> classes = Sets.newIdentityHashSet();
      classes.addAll(dependencies);

      ClassLoader classLoader = getClassLoader();
      for (RuntimeSpecification spec : twillSpec.getRunnables().values()) {
        classes.add(classLoader.loadClass(spec.getRunnableSpecification().getClassName()));
      }

      // Add the TwillRunnableEventHandler class
      if (twillSpec.getEventHandler() != null) {
        classes.add(getClassLoader().loadClass(twillSpec.getEventHandler().getClassName()));
      }

      // The location name is computed from the MD5 of all the classes names
      // The localized name is always APPLICATION_JAR
      List<String> classList = Lists.newArrayList(Iterables.transform(classes, CLASS_TO_NAME));
      Collections.sort(classList);
      Hasher hasher = Hashing.md5().newHasher();
      for (String name : classList) {
        hasher.putString(name);
      }
      // TODO: TWILL-207. Only depends on class list so that it can be reused across different programs of the same type
      String name = hasher.hash().toString() + "-" + Constants.Files.APPLICATION_JAR;

      LOG.debug("Create and copy {}", Constants.Files.APPLICATION_JAR);
      Location location = locationCache.get(name, new LocationCache.Loader() {
        @Override
        public void load(String name, Location targetLocation) throws IOException {
          bundler.createBundle(targetLocation, classes);
        }
      });

      LOG.debug("Done {}", Constants.Files.APPLICATION_JAR);

      localFiles.put(Constants.Files.APPLICATION_JAR, createLocalFile(Constants.Files.APPLICATION_JAR, location, true));

    } catch (ClassNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

  private void createResourcesJar(ApplicationBundler bundler, Map<String, LocalFile> localFiles) throws IOException {
    // If there is no resources, no need to create the jar file.
    if (resources.isEmpty()) {
      return;
    }

    LOG.debug("Create and copy {}", Constants.Files.RESOURCES_JAR);
    Location location = createTempLocation(Constants.Files.RESOURCES_JAR);
    bundler.createBundle(location, Collections.<Class<?>>emptyList(), resources);
    LOG.debug("Done {}", Constants.Files.RESOURCES_JAR);
    localFiles.put(Constants.Files.RESOURCES_JAR, createLocalFile(Constants.Files.RESOURCES_JAR, location, true));
  }

  private void createRuntimeConfigJar(Path dir, Map<String, LocalFile> localFiles) throws IOException {
    LOG.debug("Create and copy {}", Constants.Files.RUNTIME_CONFIG_JAR);

    // Jar everything under the given directory, which contains different files needed by AM/runnable containers
    Location location = createTempLocation(Constants.Files.RUNTIME_CONFIG_JAR);
    try (
      JarOutputStream jarOutput = new JarOutputStream(location.getOutputStream());
      DirectoryStream<Path> stream = Files.newDirectoryStream(dir)
    ) {
      for (Path path : stream) {
        jarOutput.putNextEntry(new JarEntry(path.getFileName().toString()));
        Files.copy(path, jarOutput);
        jarOutput.closeEntry();
      }
    }

    LOG.debug("Done {}", Constants.Files.RUNTIME_CONFIG_JAR);
    localFiles.put(Constants.Files.RUNTIME_CONFIG_JAR,
                   createLocalFile(Constants.Files.RUNTIME_CONFIG_JAR, location, true));
  }

  /**
   * Based on the given {@link TwillSpecification}, upload LocalFiles to Yarn Cluster.
   * @param twillSpec The {@link TwillSpecification} for populating resource.
   */
  private Multimap<String, LocalFile> populateRunnableLocalFiles(TwillSpecification twillSpec) throws IOException {
    Multimap<String, LocalFile> localFiles = HashMultimap.create();

    LOG.debug("Populating Runnable LocalFiles");
    for (Map.Entry<String, RuntimeSpecification> entry: twillSpec.getRunnables().entrySet()) {
      String runnableName = entry.getKey();
      for (LocalFile localFile : entry.getValue().getLocalFiles()) {
        Location location;

        URI uri = localFile.getURI();
        if (appLocation.toURI().getScheme().equals(uri.getScheme())) {
          // If the source file location is having the same scheme as the target location, no need to copy
          location = appLocation.getLocationFactory().create(uri);
        } else {
          URL url = uri.toURL();
          LOG.debug("Create and copy {} : {}", runnableName, url);
          // Preserves original suffix for expansion.
          location = copyFromURL(url, createTempLocation(Paths.addExtension(url.getFile(), localFile.getName())));
          LOG.debug("Done {} : {}", runnableName, url);
        }

        localFiles.put(runnableName,
                       new DefaultLocalFile(localFile.getName(), location.toURI(), location.lastModified(),
                                            location.length(), localFile.isArchive(), localFile.getPattern()));
      }
    }
    LOG.debug("Done Runnable LocalFiles");
    return localFiles;
  }

  private void saveSpecification(TwillSpecification spec, Path targetFile) throws IOException {
    final Multimap<String, LocalFile> runnableLocalFiles = populateRunnableLocalFiles(spec);

    // Rewrite LocalFiles inside twillSpec
    Map<String, RuntimeSpecification> runtimeSpec = Maps.transformEntries(
      spec.getRunnables(), new Maps.EntryTransformer<String, RuntimeSpecification, RuntimeSpecification>() {
        @Override
        public RuntimeSpecification transformEntry(String key, RuntimeSpecification value) {
          return new DefaultRuntimeSpecification(value.getName(), value.getRunnableSpecification(),
                                                 value.getResourceSpecification(), runnableLocalFiles.get(key));
        }
      });

    // Serialize into a local temp file.
    LOG.debug("Creating {}", targetFile);
    try (Writer writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8)) {
      EventHandlerSpecification eventHandler = spec.getEventHandler();
      if (eventHandler == null) {
        eventHandler = new LogOnlyEventHandler().configure();
      }
      TwillSpecification newTwillSpec = new DefaultTwillSpecification(spec.getName(), runtimeSpec, spec.getOrders(),
                                                                      spec.getPlacementPolicies(), eventHandler);
      TwillRuntimeSpecificationAdapter.create().toJson(
        new TwillRuntimeSpecification(newTwillSpec, appLocation.getLocationFactory().getHomeLocation().getName(),
                                      appLocation.toURI(), zkConnectString, runId, twillSpec.getName(),
                                      reservedMemory, yarnConfig.get(YarnConfiguration.RM_SCHEDULER_ADDRESS),
                                      logLevels, maxRetries, minHeapRatio), writer);
    }
    LOG.debug("Done {}", targetFile);
  }

  private void saveLogback(Path targetFile) throws IOException {
    URL url = getClass().getClassLoader().getResource(Constants.Files.LOGBACK_TEMPLATE);
    if (url == null) {
      return;
    }

    LOG.debug("Creating {}", targetFile);
    try (InputStream is = url.openStream()) {
      Files.copy(is, targetFile);
    }
    LOG.debug("Done {}", targetFile);
  }

  /**
   * Creates the launcher.jar for launch the main application.
   */
  private void createLauncherJar(Map<String, LocalFile> localFiles) throws URISyntaxException, IOException {

    LOG.debug("Create and copy {}", Constants.Files.LAUNCHER_JAR);

    Location location = locationCache.get(Constants.Files.LAUNCHER_JAR, new LocationCache.Loader() {
      @Override
      public void load(String name, Location targetLocation) throws IOException {
        // Create a jar file with the TwillLauncher and FindFreePort and dependent classes inside.
        try (JarOutputStream jarOut = new JarOutputStream(targetLocation.getOutputStream())) {
          ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
          if (classLoader == null) {
            classLoader = getClass().getClassLoader();
          }
          Dependencies.findClassDependencies(classLoader, new ClassAcceptor() {
            @Override
            public boolean accept(String className, URL classUrl, URL classPathUrl) {
              try {
                jarOut.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
                try (InputStream is = classUrl.openStream()) {
                  ByteStreams.copy(is, jarOut);
                }
              } catch (IOException e) {
                throw Throwables.propagate(e);
              }
              return true;
            }
          }, TwillLauncher.class.getName(), FindFreePort.class.getName());
        }
      }
    });

    LOG.debug("Done {}", Constants.Files.LAUNCHER_JAR);

    localFiles.put(Constants.Files.LAUNCHER_JAR, createLocalFile(Constants.Files.LAUNCHER_JAR, location));
  }

  private void saveClassPaths(Path targetDir) throws IOException {
    Files.write(targetDir.resolve(Constants.Files.APPLICATION_CLASSPATH),
                Joiner.on(':').join(applicationClassPaths).getBytes(StandardCharsets.UTF_8));
    Files.write(targetDir.resolve(Constants.Files.CLASSPATH),
                Joiner.on(':').join(classPaths).getBytes(StandardCharsets.UTF_8));
  }

  private void saveJvmOptions(final Path targetPath) throws IOException {
    if ((extraOptions == null || extraOptions.isEmpty()) &&
      JvmOptions.DebugOptions.NO_DEBUG.equals(debugOptions)) {
      // If no vm options, no need to localize the file.
      return;
    }
    LOG.debug("Creating {}", targetPath);
    JvmOptionsCodec.encode(new JvmOptions(extraOptions, debugOptions), new OutputSupplier<Writer>() {
      @Override
      public Writer getOutput() throws IOException {
        return Files.newBufferedWriter(targetPath, StandardCharsets.UTF_8);
      }
    });
    LOG.debug("Done {}", targetPath);
  }

  private void saveArguments(Arguments arguments, final Path targetPath) throws IOException {
    LOG.debug("Creating {}", targetPath);
    ArgumentsCodec.encode(arguments, new OutputSupplier<Writer>() {
      @Override
      public Writer getOutput() throws IOException {
        return Files.newBufferedWriter(targetPath, StandardCharsets.UTF_8);
      }
    });
    LOG.debug("Done {}", targetPath);
  }

  private void saveEnvironments(Path targetPath) throws IOException {
    if (environments.isEmpty()) {
      return;
    }

    LOG.debug("Creating {}", targetPath);
    try (Writer writer = Files.newBufferedWriter(targetPath, StandardCharsets.UTF_8)) {
      new Gson().toJson(environments, writer);
    }
    LOG.debug("Done {}", targetPath);
  }

  /**
   * Serializes the information for files that are localized to all YARN containers.
   */
  private void createLocalizeFilesJson(Map<String, LocalFile> localFiles) throws IOException {
    LOG.debug("Create and copy {}", Constants.Files.LOCALIZE_FILES);
    Location location = createTempLocation(Constants.Files.LOCALIZE_FILES);

    // Serialize the list of LocalFiles, except the one we are generating here, as this file is used by AM only.
    // This file should never use LocationCache.
    try (Writer writer = new OutputStreamWriter(location.getOutputStream(), StandardCharsets.UTF_8)) {
      new GsonBuilder().registerTypeAdapter(LocalFile.class, new LocalFileCodec())
        .create().toJson(localFiles.values(), new TypeToken<List<LocalFile>>() {
      }.getType(), writer);
    }
    LOG.debug("Done {}", Constants.Files.LOCALIZE_FILES);
    localFiles.put(Constants.Files.LOCALIZE_FILES, createLocalFile(Constants.Files.LOCALIZE_FILES, location));
  }

  private Location copyFromURL(URL url, Location target) throws IOException {
    try (
      InputStream is = url.openStream();
      OutputStream os = new BufferedOutputStream(target.getOutputStream())
    ) {
      ByteStreams.copy(is, os);
    }
    return target;
  }

  private Location createTempLocation(String fileName) {
    String name;
    String suffix = Paths.getExtension(fileName);

    name = fileName.substring(0, fileName.length() - suffix.length() - 1);

    try {
      return appLocation.append(name).getTempFile('.' + suffix);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Returns the context ClassLoader if there is any, otherwise, returns ClassLoader of this class.
   */
  private ClassLoader getClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return classLoader == null ? getClass().getClassLoader() : classLoader;
  }

  private ApplicationBundler createBundler(ClassAcceptor classAcceptor) {
    return new ApplicationBundler(classAcceptor).setTempDir(localStagingDir);
  }

  /**
   * Creates a {@link ApplicationBundler} for building application jar. The bundler will include classes
   * accepted by the given {@link ClassAcceptor}, as long as it is not a twill class.
   */
  private ApplicationBundler createApplicationJarBundler(final ClassAcceptor classAcceptor) {
    // Accept classes based on the classAcceptor and also excluding all twill classes as they are already
    // in the twill.jar
    return createBundler(new ClassAcceptor() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        return !twillClassPaths.contains(classPathUrl) && classAcceptor.accept(className, classUrl, classPathUrl);
      }
    });
  }
}
