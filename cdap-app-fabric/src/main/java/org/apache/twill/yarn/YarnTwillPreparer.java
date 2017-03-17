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

import co.cask.cdap.common.io.Locations;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
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
import com.google.common.io.ByteStreams;
import com.google.common.io.OutputSupplier;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ClassAcceptor;
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
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.Arguments;
import org.apache.twill.internal.Configs;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultLocalFile;
import org.apache.twill.internal.DefaultRuntimeSpecification;
import org.apache.twill.internal.DefaultTwillSpecification;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.JvmOptions;
import org.apache.twill.internal.LogOnlyEventHandler;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.ProcessLauncher;
import org.apache.twill.internal.RunIds;
import org.apache.twill.internal.appmaster.ApplicationMasterInfo;
import org.apache.twill.internal.appmaster.ApplicationMasterMain;
import org.apache.twill.internal.container.TwillContainerMain;
import org.apache.twill.internal.json.ArgumentsCodec;
import org.apache.twill.internal.json.JvmOptionsCodec;
import org.apache.twill.internal.json.LocalFileCodec;
import org.apache.twill.internal.json.TwillSpecificationAdapter;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Implementation for {@link TwillPreparer} to prepare and launch distributed application on Hadoop YARN.
 */
final class YarnTwillPreparer implements TwillPreparer {

  private static final Logger LOG = LoggerFactory.getLogger(YarnTwillPreparer.class);

  private final YarnConfiguration yarnConfig;
  private final TwillSpecification twillSpec;
  private final YarnAppClient yarnAppClient;
  private final String zkConnectString;
  private final LocationFactory locationFactory;
  private final YarnTwillControllerFactory controllerFactory;
  private final RunId runId;

  private final List<LogHandler> logHandlers = Lists.newArrayList();
  private final List<String> arguments = Lists.newArrayList();
  private final Set<Class<?>> dependencies = Sets.newIdentityHashSet();
  private final List<URI> resources = Lists.newArrayList();
  private final List<String> classPaths = Lists.newArrayList();
  private final ListMultimap<String, String> runnableArgs = ArrayListMultimap.create();
  private final Map<String, Map<String, String>> environments = new HashMap<>();
  private final List<String> applicationClassPaths = Lists.newArrayList();
  private final Credentials credentials;
  private final int reservedMemory;
  private final double heapMinRatio;
  private String user;
  private String schedulerQueue;
  private String extraOptions;
  private JvmOptions.DebugOptions debugOptions = JvmOptions.DebugOptions.NO_DEBUG;
  private ClassAcceptor classAcceptor;
  private LogEntry.Level logLevel;
  // Hack for CDAP-7021
  private LocationFactory jarCacheLocationFactory;
  // Hacks for TWILL-187
  private Integer maxStartSeconds;
  private Integer maxStopSeconds;

  YarnTwillPreparer(YarnConfiguration yarnConfig, TwillSpecification twillSpec,
                    YarnAppClient yarnAppClient, String zkConnectString,
                    LocationFactory locationFactory, String extraOptions, LogEntry.Level logLevel,
                    YarnTwillControllerFactory controllerFactory) {
    this.yarnConfig = yarnConfig;
    this.twillSpec = twillSpec;
    this.yarnAppClient = yarnAppClient;
    this.zkConnectString = zkConnectString;
    this.locationFactory = locationFactory;
    this.controllerFactory = controllerFactory;
    this.runId = RunIds.generate();
    this.credentials = createCredentials();
    this.reservedMemory = yarnConfig.getInt(Configs.Keys.JAVA_RESERVED_MEMORY_MB,
                                            Configs.Defaults.JAVA_RESERVED_MEMORY_MB);
    // Back-porting the feature introduced in TWILL-216
    this.heapMinRatio = yarnConfig.getDouble(co.cask.cdap.common.conf.Constants.CFG_TWILL_HEAP_RESERVED_MIN_RATIO,
                                             Constants.HEAP_MIN_RATIO);
    this.user = System.getProperty("user.name");
    this.extraOptions = extraOptions;
    this.logLevel = logLevel;
    this.classAcceptor = new ClassAcceptor();
  }

  @Override
  public TwillPreparer addLogHandler(LogHandler handler) {
    logHandlers.add(handler);
    return this;
  }

  @Override
  public TwillPreparer setUser(String user) {
    this.user = user;
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
    for (String arg : args) {
      // Hack for CDAP-7021 and TWILL-187
      if (arg.startsWith("cdap.jar.cache.dir=")) {
        String jarCacheDir = arg.substring(arg.indexOf("=") + 1);
        LOG.debug("using local directory {} to cache jars.", jarCacheDir);
        jarCacheLocationFactory = new LocalLocationFactory(new File(jarCacheDir));
      } else if (arg.startsWith("app.max.start.seconds=")) {
        String secondsStr = arg.substring(arg.indexOf("=") + 1);
        try {
          maxStartSeconds = Integer.parseInt(secondsStr);
          if (maxStartSeconds < 0) {
            LOG.warn("Invalid value for app.max.start.seconds={}. The default value will be used.", secondsStr);
            maxStartSeconds = null;
          }
          LOG.debug("using max start seconds {}.", maxStartSeconds);
        } catch (NumberFormatException e) {
          LOG.warn("Invalid value for app.max.start.seconds={}. The default value will be used.", secondsStr);
        }
      } else if (arg.startsWith("app.max.stop.seconds=")) {
        String secondsStr = arg.substring(arg.indexOf("=") + 1);
        try {
          maxStopSeconds = Integer.parseInt(secondsStr);
          if (maxStopSeconds < 0) {
            LOG.warn("Invalid value for app.max.stop.seconds={}. The default value will be used.", secondsStr);
            maxStopSeconds = null;
          }
          LOG.debug("using max stop seconds {}.", maxStopSeconds);
        } catch (NumberFormatException e) {
          LOG.warn("Invalid value for app.max.stop.seconds={}. The default value will be used.", secondsStr);
        }
      } else {
        arguments.add(arg);
      }
    }
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    return withArguments(runnableName, ImmutableList.copyOf(args));
  }

  @Override
  public TwillPreparer withArguments(String runnableName, Iterable<String> args) {
    Preconditions.checkArgument(twillSpec.getRunnables().containsKey(runnableName),
                                "Runnable %s is not defined in the application.", runnableName);
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
    Preconditions.checkArgument(twillSpec.getRunnables().containsKey(runnableName),
                                "Runnable %s is not defined in the application.", runnableName);
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
  public TwillPreparer addSecureStore(SecureStore secureStore) {
    Object store = secureStore.getStore();
    Preconditions.checkArgument(store instanceof Credentials, "Only Hadoop Credentials is supported.");
    this.credentials.mergeAll((Credentials) store);
    return this;
  }

  @Override
  public TwillPreparer setLogLevel(LogEntry.Level logLevel) {
    Preconditions.checkNotNull(logLevel);
    this.logLevel = logLevel;
    return this;
  }

  @Override
  public TwillController start() {
    try {
      final ProcessLauncher<ApplicationMasterInfo> launcher = yarnAppClient.createLauncher(twillSpec, schedulerQueue);
      final ApplicationMasterInfo appMasterInfo = launcher.getContainerInfo();
      Callable<ProcessController<YarnApplicationReport>> submitTask =
        new Callable<ProcessController<YarnApplicationReport>>() {
          @Override
          public ProcessController<YarnApplicationReport> call() throws Exception {
            String fsUser = locationFactory.getHomeLocation().getName();

            // Local files needed by AM
            Map<String, LocalFile> localFiles = Maps.newHashMap();
            // Local files declared by runnables
            Multimap<String, LocalFile> runnableLocalFiles = HashMultimap.create();

            createAppMasterJar(createBundler(), localFiles);
            createContainerJar(createBundler(), localFiles);
            populateRunnableLocalFiles(twillSpec, runnableLocalFiles);
            saveSpecification(twillSpec, runnableLocalFiles, localFiles);
            saveLogback(localFiles);
            saveLauncher(localFiles);
            saveJvmOptions(localFiles);
            saveArguments(new Arguments(arguments, runnableArgs), localFiles);
            saveEnvironments(localFiles);
            saveLocalFiles(localFiles, ImmutableSet.of(Constants.Files.TWILL_SPEC,
                                                       Constants.Files.LOGBACK_TEMPLATE,
                                                       Constants.Files.CONTAINER_JAR,
                                                       Constants.Files.LAUNCHER_JAR,
                                                       Constants.Files.ARGUMENTS));

            LOG.debug("Submit AM container spec: {}", appMasterInfo);
            // java -Djava.io.tmpdir=tmp -cp launcher.jar:$HADOOP_CONF_DIR -XmxMemory
            //     org.apache.twill.internal.TwillLauncher
            //     appMaster.jar
            //     org.apache.twill.internal.appmaster.ApplicationMasterMain
            //     false
            ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
              .put(EnvKeys.TWILL_FS_USER, fsUser)
              .put(EnvKeys.TWILL_APP_DIR, getAppLocation().toURI().toASCIIString())
              .put(EnvKeys.TWILL_ZK_CONNECT, zkConnectString)
              .put(EnvKeys.TWILL_RUN_ID, runId.getId())
              .put(EnvKeys.TWILL_RESERVED_MEMORY_MB, Integer.toString(reservedMemory))
              .put(EnvKeys.TWILL_APP_NAME, twillSpec.getName())
              .put(EnvKeys.YARN_RM_SCHEDULER_ADDRESS, yarnConfig.get(YarnConfiguration.RM_SCHEDULER_ADDRESS));

            LOG.debug("Log level is set to {} for the Twill application.", logLevel);
            builder.put(EnvKeys.TWILL_APP_LOG_LEVEL, logLevel.toString());

            // Back-porting the feature introduced in TWILL-216
            builder.put(co.cask.cdap.common.conf.Constants.ENV_TWILL_HEAP_RESERVED_MIN_RATIO,
                        Double.toString(heapMinRatio));

            int reservedMemoryMB = yarnConfig.getInt(
              co.cask.cdap.common.conf.Constants.CFG_TWILL_YARN_AM_RESERVED_MEMORY_MB,
              co.cask.cdap.common.conf.Constants.DEFAULT_TWILL_YARN_AM_RESERVED_MEMORY_MB);
            int memory = Resources.computeMaxHeapSize(appMasterInfo.getMemoryMB(), reservedMemoryMB, heapMinRatio);
            return launcher.prepareLaunch(builder.build(), localFiles.values(), credentials)
              .addCommand(
                "$JAVA_HOME/bin/java",
                "-Djava.io.tmpdir=tmp",
                "-Dyarn.appId=$" + EnvKeys.YARN_APP_ID_STR,
                "-Dtwill.app=$" + EnvKeys.TWILL_APP_NAME,
                "-cp", Constants.Files.LAUNCHER_JAR + ":$HADOOP_CONF_DIR",
                "-Xmx" + memory + "m",
                extraOptions == null ? "" : extraOptions,
                TwillLauncher.class.getName(),
                Constants.Files.APP_MASTER_JAR,
                ApplicationMasterMain.class.getName(),
                Boolean.FALSE.toString())
              .launch();
          }
        };

      YarnTwillController controller = controllerFactory.create(runId, logHandlers, submitTask);
      // Hacks for TWILL-187
      if (maxStartSeconds != null) {
        controller.setMaxStartSeconds(maxStartSeconds);
      }
      if (maxStopSeconds != null) {
        controller.setMaxStopSeconds(maxStopSeconds);
      }
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

  private Credentials createCredentials() {
    Credentials credentials = new Credentials();

    try {
      credentials.addAll(UserGroupInformation.getCurrentUser().getCredentials());

      List<Token<?>> tokens = YarnUtils.addDelegationTokens(yarnConfig, locationFactory, credentials);
      for (Token<?> token : tokens) {
        LOG.debug("Delegation token acquired for {}, {}", locationFactory.getHomeLocation(), token);
      }
    } catch (IOException e) {
      LOG.warn("Failed to check for secure login type. Not gathering any delegation token.", e);
    }
    return credentials;
  }

  private ApplicationBundler createBundler() {
    return new ApplicationBundler(classAcceptor);
  }

  private LocalFile createLocalFile(String name, Location location) throws IOException {
    return createLocalFile(name, location, false);
  }

  private LocalFile createLocalFile(String name, Location location, boolean archive) throws IOException {
    return new DefaultLocalFile(name, location.toURI(), location.lastModified(), location.length(), archive, null);
  }

  private void createAppMasterJar(ApplicationBundler bundler, Map<String, LocalFile> localFiles) throws IOException {
    try {
      LOG.debug("Create and copy {}", Constants.Files.APP_MASTER_JAR);
      Location location = createTempLocation(Constants.Files.APP_MASTER_JAR);

      Location cachedLocation = jarCacheLocationFactory == null ?
        null : jarCacheLocationFactory.create(Constants.Files.APP_MASTER_JAR);
      Location cacheDoneLocation = cachedLocation == null ?
        null : jarCacheLocationFactory.create(Constants.Files.APP_MASTER_JAR + ".done");

      if (cachedLocation != null && cacheDoneLocation.exists()) {
        LOG.debug("Found cached app master jar for twill app {} at {}", twillSpec.getName(), cachedLocation);
        // the jar is cached on local disk. Upload it to hdfs.
        ByteStreams.copy(Locations.newInputSupplier(cachedLocation), Locations.newOutputSupplier(location));
      } else {
        List<Class<?>> classes = Lists.newArrayList();
        classes.add(ApplicationMasterMain.class);

        // Stuck in the yarnAppClient class to make bundler being able to pickup the right yarn-client version
        classes.add(yarnAppClient.getClass());

        // Add the TwillRunnableEventHandler class
        if (twillSpec.getEventHandler() != null) {
          classes.add(getClassLoader().loadClass(twillSpec.getEventHandler().getClassName()));
        }

        bundler.createBundle(location, classes);
        LOG.debug("Done {}", Constants.Files.APP_MASTER_JAR);

        if (cachedLocation != null) {
          copyToLocalCache(location, cachedLocation, cacheDoneLocation);
        }
      }
      localFiles.put(Constants.Files.APP_MASTER_JAR, createLocalFile(Constants.Files.APP_MASTER_JAR, location));

    } catch (ClassNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

  private void createContainerJar(ApplicationBundler bundler, Map<String, LocalFile> localFiles) throws IOException {
    try {
      LOG.debug("Create and copy {}", Constants.Files.CONTAINER_JAR);
      Location location = createTempLocation(Constants.Files.CONTAINER_JAR);

      Location cachedLocation = jarCacheLocationFactory == null ?
        null : jarCacheLocationFactory.create(Constants.Files.CONTAINER_JAR);
      Location cacheDoneLocation = cachedLocation == null ?
        null : jarCacheLocationFactory.create(Constants.Files.CONTAINER_JAR + ".done");
      if (cachedLocation != null && cacheDoneLocation.exists()) {
        LOG.debug("Found cached container jar for twill app {} at {}", twillSpec.getName(), cachedLocation);
        // the jar is cached on local disk. Upload it to hdfs.
        ByteStreams.copy(Locations.newInputSupplier(cachedLocation), Locations.newOutputSupplier(location));
      } else {
        Set<Class<?>> classes = Sets.newIdentityHashSet();
        classes.add(TwillContainerMain.class);
        classes.addAll(dependencies);

        ClassLoader classLoader = getClassLoader();
        for (RuntimeSpecification spec : twillSpec.getRunnables().values()) {
          classes.add(classLoader.loadClass(spec.getRunnableSpecification().getClassName()));
        }

        bundler.createBundle(location, classes, resources);
        LOG.debug("Done {}", Constants.Files.CONTAINER_JAR);

        if (cachedLocation != null) {
          copyToLocalCache(location, cachedLocation, cacheDoneLocation);
        }
      }

      localFiles.put(Constants.Files.CONTAINER_JAR, createLocalFile(Constants.Files.CONTAINER_JAR, location));

    } catch (ClassNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Based on the given {@link TwillSpecification}, upload LocalFiles to Yarn Cluster.
   * @param twillSpec The {@link TwillSpecification} for populating resource.
   * @param localFiles A Multimap to store runnable name to transformed LocalFiles.
   * @throws IOException
   */
  private void populateRunnableLocalFiles(TwillSpecification twillSpec,
                                          Multimap<String, LocalFile> localFiles) throws IOException {

    LOG.debug("Populating Runnable LocalFiles");
    for (Map.Entry<String, RuntimeSpecification> entry: twillSpec.getRunnables().entrySet()) {
      String runnableName = entry.getKey();
      for (LocalFile localFile : entry.getValue().getLocalFiles()) {
        Location location;

        URI uri = localFile.getURI();
        if (locationFactory.getHomeLocation().toURI().getScheme().equals(uri.getScheme())) {
          // If the source file location is having the same scheme as the target location, no need to copy
          location = locationFactory.create(uri);
        } else {
          URL url = uri.toURL();
          LOG.debug("Create and copy {} : {}", runnableName, url);
          // Preserves original suffix for expansion.
          location = copyFromURL(url, createTempLocation(Paths.appendSuffix(url.getFile(), localFile.getName())));
          LOG.debug("Done {} : {}", runnableName, url);
        }

        localFiles.put(runnableName,
                       new DefaultLocalFile(localFile.getName(), location.toURI(), location.lastModified(),
                                            location.length(), localFile.isArchive(), localFile.getPattern()));
      }
    }
    LOG.debug("Done Runnable LocalFiles");
  }

  private void saveSpecification(TwillSpecification spec, final Multimap<String, LocalFile> runnableLocalFiles,
                                 Map<String, LocalFile> localFiles) throws IOException {
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
    LOG.debug("Create and copy {}", Constants.Files.TWILL_SPEC);
    Location location = createTempLocation(Constants.Files.TWILL_SPEC);
    try (Writer writer = new OutputStreamWriter(location.getOutputStream(), Charsets.UTF_8)) {
      EventHandlerSpecification eventHandler = spec.getEventHandler();
      if (eventHandler == null) {
        eventHandler = new LogOnlyEventHandler().configure();
      }

      TwillSpecificationAdapter.create().toJson(
        new DefaultTwillSpecification(spec.getName(), runtimeSpec, spec.getOrders(), spec.getPlacementPolicies(),
                                      eventHandler), writer);
    }
    LOG.debug("Done {}", Constants.Files.TWILL_SPEC);

    localFiles.put(Constants.Files.TWILL_SPEC, createLocalFile(Constants.Files.TWILL_SPEC, location));
  }

  private void saveLogback(Map<String, LocalFile> localFiles) throws IOException {
    LOG.debug("Create and copy {}", Constants.Files.LOGBACK_TEMPLATE);
    Location location = copyFromURL(getClass().getClassLoader().getResource(Constants.Files.LOGBACK_TEMPLATE),
                                    createTempLocation(Constants.Files.LOGBACK_TEMPLATE));
    LOG.debug("Done {}", Constants.Files.LOGBACK_TEMPLATE);

    localFiles.put(Constants.Files.LOGBACK_TEMPLATE, createLocalFile(Constants.Files.LOGBACK_TEMPLATE, location));
  }

  /**
   * Creates the launcher.jar for launch the main application.
   */
  private void saveLauncher(Map<String, LocalFile> localFiles) throws URISyntaxException, IOException {

    LOG.debug("Create and copy {}", Constants.Files.LAUNCHER_JAR);
    Location location = createTempLocation(Constants.Files.LAUNCHER_JAR);

    final String launcherName = TwillLauncher.class.getName();
    final String portFinderName = FindFreePort.class.getName();

    // Create a jar file with the TwillLauncher optionally a json serialized classpath.json in it.
    // Also a little utility to find a free port, used for debugging.
    final JarOutputStream jarOut = new JarOutputStream(location.getOutputStream());
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = getClass().getClassLoader();
    }
    Dependencies.findClassDependencies(classLoader, new ClassAcceptor() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        Preconditions.checkArgument(className.startsWith(launcherName) || className.equals(portFinderName),
                                    "Launcher jar should not have dependencies: %s", className);
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
    }, launcherName, portFinderName);

    try {
      addClassPaths(Constants.CLASSPATH, classPaths, jarOut);
      addClassPaths(Constants.APPLICATION_CLASSPATH, applicationClassPaths, jarOut);
    } finally {
      jarOut.close();
    }
    LOG.debug("Done {}", Constants.Files.LAUNCHER_JAR);

    localFiles.put(Constants.Files.LAUNCHER_JAR, createLocalFile(Constants.Files.LAUNCHER_JAR, location));
  }

  private void addClassPaths(String classpathId, List<String> classPaths, JarOutputStream jarOut) throws IOException {
    if (!classPaths.isEmpty()) {
      jarOut.putNextEntry(new JarEntry(classpathId));
      jarOut.write(Joiner.on(':').join(classPaths).getBytes(Charsets.UTF_8));
    }
  }

  private void saveJvmOptions(Map<String, LocalFile> localFiles) throws IOException {
    if ((extraOptions == null || extraOptions.isEmpty()) &&
      JvmOptions.DebugOptions.NO_DEBUG.equals(debugOptions)) {
      // If no vm options, no need to localize the file.
      return;
    }
    LOG.debug("Create and copy {}", Constants.Files.JVM_OPTIONS);
    final Location location = createTempLocation(Constants.Files.JVM_OPTIONS);
    JvmOptionsCodec.encode(new JvmOptions(extraOptions, debugOptions), new OutputSupplier<Writer>() {
      @Override
      public Writer getOutput() throws IOException {
        return new OutputStreamWriter(location.getOutputStream(), Charsets.UTF_8);
      }
    });
    LOG.debug("Done {}", Constants.Files.JVM_OPTIONS);

    localFiles.put(Constants.Files.JVM_OPTIONS, createLocalFile(Constants.Files.JVM_OPTIONS, location));
  }

  private void saveArguments(Arguments arguments, Map<String, LocalFile> localFiles) throws IOException {
    LOG.debug("Create and copy {}", Constants.Files.ARGUMENTS);
    final Location location = createTempLocation(Constants.Files.ARGUMENTS);
    ArgumentsCodec.encode(arguments, new OutputSupplier<Writer>() {
      @Override
      public Writer getOutput() throws IOException {
        return new OutputStreamWriter(location.getOutputStream(), Charsets.UTF_8);
      }
    });
    LOG.debug("Done {}", Constants.Files.ARGUMENTS);

    localFiles.put(Constants.Files.ARGUMENTS, createLocalFile(Constants.Files.ARGUMENTS, location));
  }

  private void saveEnvironments(Map<String, LocalFile> localFiles) throws IOException {
    if (environments.isEmpty()) {
      return;
    }

    LOG.debug("Create and copy {}", Constants.Files.ENVIRONMENTS);
    final Location location = createTempLocation(Constants.Files.ENVIRONMENTS);
    try (Writer writer = new OutputStreamWriter(location.getOutputStream(), Charsets.UTF_8)) {
      new Gson().toJson(environments, writer);
    }
    LOG.debug("Done {}", Constants.Files.ENVIRONMENTS);

    localFiles.put(Constants.Files.ENVIRONMENTS, createLocalFile(Constants.Files.ENVIRONMENTS, location));
  }

  /**
   * Serializes the list of files that needs to localize from AM to Container.
   */
  private void saveLocalFiles(Map<String, LocalFile> localFiles, Set<String> includes) throws IOException {
    Map<String, LocalFile> localize = ImmutableMap.copyOf(Maps.filterKeys(localFiles, Predicates.in(includes)));
    LOG.debug("Create and copy {}", Constants.Files.LOCALIZE_FILES);
    Location location = createTempLocation(Constants.Files.LOCALIZE_FILES);
    try (Writer writer = new OutputStreamWriter(location.getOutputStream(), Charsets.UTF_8)) {
      new GsonBuilder().registerTypeAdapter(LocalFile.class, new LocalFileCodec())
        .create().toJson(localize.values(), new TypeToken<List<LocalFile>>() {
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
      return getAppLocation().append(name).getTempFile('.' + suffix);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private Location getAppLocation() {
    return locationFactory.create(String.format("/%s/%s", twillSpec.getName(), runId.getId()));
  }

  /**
   * Returns the context ClassLoader if there is any, otherwise, returns ClassLoader of this class.
   */
  private ClassLoader getClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return classLoader == null ? getClass().getClassLoader() : classLoader;
  }

  // part of hack for CDAP-7021
  private void copyToLocalCache(Location jarLocation, Location cacheLocation,
                                Location doneLocation) throws IOException {

    // to handle race conditions, only the first one to create the cached location will populate it
    if (cacheLocation.createNew()) {
      try {
        ByteStreams.copy(Locations.newInputSupplier(jarLocation), Locations.newOutputSupplier(cacheLocation));
      } catch (IOException e) {
        LOG.error("Error caching container jar for twill app {} at {}, it will need to be rebuilt next time.",
                  twillSpec.getName(), cacheLocation, e);
        cacheLocation.delete();
        return;
      }
      try {
        doneLocation.createNew();
      } catch (IOException e) {
        LOG.error("Error caching container jar for twill app {} at {}, it will need to be rebuilt next time.",
                  twillSpec.getName(), cacheLocation, e);
        try {
          doneLocation.delete();
        } catch (IOException e1) {
          LOG.error("Error cleaning up {} after cache failure. The file will need to be manually deleted.",
                    doneLocation, e1);
        } finally {
          try {
            cacheLocation.delete();
          } catch (IOException e2) {
            LOG.error("Error cleaning up {} after cache failure. The file will need to be manually deleted.",
                      cacheLocation, e2);
          }
        }
      }
    }
  }
}
