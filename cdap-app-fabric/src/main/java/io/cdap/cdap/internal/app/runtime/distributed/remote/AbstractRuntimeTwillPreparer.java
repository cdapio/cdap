/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.Arguments;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultLocalFile;
import org.apache.twill.internal.DefaultRuntimeSpecification;
import org.apache.twill.internal.DefaultTwillSpecification;
import org.apache.twill.internal.JvmOptions;
import org.apache.twill.internal.LogOnlyEventHandler;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.io.LocationCache;
import org.apache.twill.internal.json.ArgumentsCodec;
import org.apache.twill.internal.json.TwillRuntimeSpecificationAdapter;
import org.apache.twill.internal.utils.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;

/**
 * An abstract base implementation for implementing {@link TwillPreparer} for program runtime.
 */
abstract class AbstractRuntimeTwillPreparer implements TwillPreparer {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractRuntimeTwillPreparer.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final TwillSpecification twillSpec;
  private final ProgramRunId programRunId;
  private final ProgramOptions programOptions;

  private final List<String> arguments = new ArrayList<>();
  private final Set<Class<?>> dependencies = Sets.newIdentityHashSet();
  private final List<URI> resources = new ArrayList<>();
  private final List<String> classPaths = new ArrayList<>();
  private final ListMultimap<String, String> runnableArgs = ArrayListMultimap.create();
  private final Map<String, Map<String, String>> environments = new HashMap<>();
  private final List<String> applicationClassPaths = new ArrayList<>();
  private final Map<String, Map<String, String>> logLevels = new HashMap<>();
  private final LocationCache locationCache;
  private final Map<String, Integer> maxRetries = new HashMap<>();
  private final Map<String, Map<String, String>> runnableConfigs = new HashMap<>();
  private final Map<String, String> runnableExtraOptions = new HashMap<>();
  private final LocationFactory locationFactory;
  private final TwillControllerFactory controllerFactory;

  private JvmOptions.DebugOptions debugOptions = JvmOptions.DebugOptions.NO_DEBUG;
  private ClassAcceptor classAcceptor = new ClassAcceptor();
  private String extraOptions;
  private String classLoaderClassName;

  AbstractRuntimeTwillPreparer(CConfiguration cConf, Configuration hConf,
                               TwillSpecification twillSpec, ProgramRunId programRunId,
                               ProgramOptions programOptions, LocationCache locationCache,
                               LocationFactory locationFactory, TwillControllerFactory controllerFactory) {
    // Check to prevent future mistake
    if (twillSpec.getRunnables().size() != 1) {
      throw new IllegalArgumentException("Only one TwillRunnable is supported");
    }

    this.cConf = cConf;
    this.hConf = hConf;
    this.twillSpec = twillSpec;
    this.programRunId = programRunId;
    this.programOptions = programOptions;
    this.locationCache = locationCache;
    this.locationFactory = locationFactory;
    this.controllerFactory = controllerFactory;
    this.extraOptions = cConf.get(io.cdap.cdap.common.conf.Constants.AppFabric.PROGRAM_JVM_OPTS);
  }

  /**
   * Returns the {@link ProgramRunId} that is going to be launched by this preparer.
   */
  ProgramRunId getProgramRunId() {
    return programRunId;
  }

  /**
   * Returns the {@link LocationCache} used by this preparer.
   */
  LocationCache getLocationCache() {
    return locationCache;
  }

  /**
   * Returns the {@link LocationFactory} used by this preparer.
   */
  LocationFactory getLocationFactory() {
    return locationFactory;
  }

  /**
   * Launch the program based on the given set of specifications.
   *
   * @param twillRuntimeSpec the runtime specification for the twill runnable for the program
   * @param runtimeSpec the specification about the runtime environment
   * @param jvmOptions the JVM options to use
   * @param environments the environment variables to set
   * @param localFiles set of files to be localized to the remote runtime
   * @param timeoutChecker a {@link TimeoutChecker} to check for startup timeout.
   * @throws Exception if failed to launch the program
   */
  abstract void launch(TwillRuntimeSpecification twillRuntimeSpec, RuntimeSpecification runtimeSpec,
                       JvmOptions jvmOptions, Map<String, String> environments,
                       Map<String, LocalFile> localFiles, TimeoutChecker timeoutChecker) throws Exception;

  /**
   * Sub-classes can override this method to add extra files to get localized.
   *
   * @param stagingDir a local staging directory for temporary file creation
   * @param localFiles a {@link Map} for adding new files to be localized
   */
  void addLocalFiles(Path stagingDir, Map<String, LocalFile> localFiles) throws IOException {
    // no-op
  }

  /**
   * Sub-classes can override this method to add extra files that goes into the runtime config directory.
   *
   * @param runtimeConfigDir a local directory for adding extra files to
   */
  void addRuntimeConfigFiles(Path runtimeConfigDir) throws IOException {
    // no-op
  }

  /**
   * Creates an instance of {@link LocalFile} based on the parameters.
   */
  LocalFile createLocalFile(String name, Location location, boolean archive) throws IOException {
    return new DefaultLocalFile(name, location.toURI(), location.lastModified(), location.length(), archive, null);
  }

  /**
   * Creates an {@link ApplicationBundler} for creating bundle jar through dependency tracing.
   *
   * @param stagingDir a staging directory for local temporary files.
   * @return an {@link ApplicationBundler} which use the current {@link #classAcceptor} for class filtering.
   */
  ApplicationBundler createBundler(Path stagingDir) {
    return new ApplicationBundler(classAcceptor).setTempDir(stagingDir.toFile());
  }

  @Override
  public TwillPreparer withConfiguration(Map<String, String> config) {
    config.forEach(hConf::set);
    return this;
  }

  @Override
  public TwillPreparer withConfiguration(String runnableName, Map<String, String> config) {
    confirmRunnableName(runnableName);
    runnableConfigs.put(runnableName, Maps.newHashMap(config));
    return this;
  }

  @Override
  public TwillPreparer addLogHandler(LogHandler handler) {
    LOG.trace("LogHandler is not supported for {}", getClass().getSimpleName());
    return this;
  }

  @Override
  public TwillPreparer setUser(String user) {
    return this;
  }

  @Override
  public TwillPreparer setSchedulerQueue(String name) {
    LOG.trace("Scheduler queue is not supported for {}", getClass().getSimpleName());
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String options) {
    Preconditions.checkArgument(options != null, "JVM options cannot be null.");
    this.extraOptions = options;
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String runnableName, String options) {
    confirmRunnableName(runnableName);
    Preconditions.checkArgument(options != null, "JVM options cannot be null.");
    runnableExtraOptions.put(runnableName, options);
    return this;
  }

  @Override
  public TwillPreparer addJVMOptions(String options) {
    Preconditions.checkArgument(options != null, "JVM options cannot be null.");
    this.extraOptions = extraOptions.isEmpty() ? options : extraOptions + " " + options;
    return this;
  }

  @Override
  public TwillPreparer enableDebugging(String... runnables) {
    return enableDebugging(false, runnables);
  }

  @Override
  public TwillPreparer enableDebugging(boolean doSuspend, String... runnables) {
    List<String> runnableList = Arrays.asList(runnables);
    runnableList.forEach(this::confirmRunnableName);
    this.debugOptions = new JvmOptions.DebugOptions(true, doSuspend, runnableList);
    return this;
  }

  @Override
  public TwillPreparer withApplicationArguments(String... args) {
    return withApplicationArguments(Arrays.asList(args));
  }

  @Override
  public TwillPreparer withApplicationArguments(Iterable<String> args) {
    Iterables.addAll(arguments, args);
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    return withArguments(runnableName, Arrays.asList(args));
  }

  @Override
  public TwillPreparer withArguments(String runnableName, Iterable<String> args) {
    confirmRunnableName(runnableName);
    runnableArgs.putAll(runnableName, args);
    return this;
  }

  @Override
  public TwillPreparer withDependencies(Class<?>... classes) {
    return withDependencies(Arrays.asList(classes));
  }

  @Override
  public TwillPreparer withDependencies(Iterable<Class<?>> classes) {
    Iterables.addAll(dependencies, classes);
    return this;
  }

  @Override
  public TwillPreparer withResources(URI... resources) {
    return withResources(Arrays.asList(resources));
  }

  @Override
  public TwillPreparer withResources(Iterable<URI> resources) {
    Iterables.addAll(this.resources, resources);
    return this;
  }

  @Override
  public TwillPreparer withClassPaths(String... classPaths) {
    return withClassPaths(Arrays.asList(classPaths));
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
    return withApplicationClassPaths(Arrays.asList(classPaths));
  }

  @Override
  public TwillPreparer withApplicationClassPaths(Iterable<String> classPaths) {
    Iterables.addAll(this.applicationClassPaths, classPaths);
    return this;
  }

  @Override
  public TwillPreparer withBundlerClassAcceptor(ClassAcceptor classAcceptor) {
    if (classAcceptor == null) {
      throw new IllegalArgumentException("Class acceptor cannot be null");
    }
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
    return this;
  }

  @Override
  public TwillPreparer setLogLevel(LogEntry.Level logLevel) {
    return setLogLevels(Collections.singletonMap(Logger.ROOT_LOGGER_NAME, logLevel));
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
  public TwillPreparer setClassLoader(String classLoaderClassName) {
    this.classLoaderClassName = classLoaderClassName;
    return this;
  }

  @Override
  public TwillController start() {
    return start(Constants.APPLICATION_MAX_START_SECONDS, TimeUnit.SECONDS);
  }

  @Override
  public TwillController start(long timeout, TimeUnit timeoutUnit) {
    long startTime = System.currentTimeMillis();

    Callable<Void> startupTask = () -> {
      Path tempDir = java.nio.file.Paths.get(
        cConf.get(io.cdap.cdap.common.conf.Constants.CFG_LOCAL_DATA_DIR),
        cConf.get(io.cdap.cdap.common.conf.Constants.AppFabric.TEMP_DIR)).toAbsolutePath();
      Path stagingDir = Files.createTempDirectory(tempDir, programRunId.getRun());

      LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(
        programRunId, programOptions.getArguments().asMap());
      Cancellable cancelLoggingContext = LoggingContextAccessor.setLoggingContext(loggingContext);

      try {
        Map<String, LocalFile> localFiles = new HashMap<>();
        addLocalFiles(stagingDir, localFiles);
        createApplicationJar(createBundler(stagingDir), localFiles);
        createResourcesJar(createBundler(stagingDir), localFiles, stagingDir);

        throwIfTimeout(startTime, timeout, timeoutUnit);

        TwillRuntimeSpecification twillRuntimeSpec;
        Path runtimeConfigDir = Files.createTempDirectory(stagingDir, Constants.Files.RUNTIME_CONFIG_JAR);
        try {
          twillRuntimeSpec = saveSpecification(twillSpec,
                                               runtimeConfigDir.resolve(Constants.Files.TWILL_SPEC), stagingDir);
          saveLogback(runtimeConfigDir.resolve(Constants.Files.LOGBACK_TEMPLATE));
          saveClassPaths(runtimeConfigDir);
          saveArguments(new Arguments(arguments, runnableArgs), runtimeConfigDir.resolve(Constants.Files.ARGUMENTS));
          addRuntimeConfigFiles(runtimeConfigDir);
          createRuntimeConfigJar(runtimeConfigDir, localFiles, stagingDir);
        } finally {
          Paths.deleteRecursively(runtimeConfigDir);
        }

        throwIfTimeout(startTime, timeout, timeoutUnit);

        RuntimeSpecification runtimeSpec = twillRuntimeSpec.getTwillSpecification().getRunnables().values()
          .stream().findFirst().orElseThrow(IllegalStateException::new);

        launch(twillRuntimeSpec, runtimeSpec, getJvmOptions(),
               environments.getOrDefault(runtimeSpec.getName(), Collections.emptyMap()),
               localFiles, () -> throwIfTimeout(startTime, timeout, timeoutUnit));

      } finally {
        cancelLoggingContext.cancel();
        DirUtils.deleteDirectoryContents(stagingDir.toFile(), false);
      }
      return null;
    };

    return controllerFactory.create(startupTask, timeout, timeoutUnit);
  }

  private void createApplicationJar(ApplicationBundler bundler,
                                    Map<String, LocalFile> localFiles) throws IOException {
    Set<Class<?>> classes = Sets.newIdentityHashSet();
    classes.addAll(dependencies);

    try {
      ClassLoader classLoader = getClassLoader();
      for (RuntimeSpecification spec : twillSpec.getRunnables().values()) {
        classes.add(classLoader.loadClass(spec.getRunnableSpecification().getClassName()));
      }

      // Add the TwillRunnableEventHandler class
      if (twillSpec.getEventHandler() != null) {
        classes.add(getClassLoader().loadClass(twillSpec.getEventHandler().getClassName()));
      }
    } catch (ClassNotFoundException e) {
      throw new IOException("Cannot create application jar", e);
    }

    // The location name is computed from the MD5 of all the classes names
    // The localized name is always APPLICATION_JAR
    List<String> classList = classes.stream().map(Class::getName).sorted().collect(Collectors.toList());
    Hasher hasher = Hashing.md5().newHasher();
    for (String name : classList) {
      hasher.putString(name);
    }
    // Only depends on class list so that it can be reused across different launches
    String name = hasher.hash().toString() + "-" + Constants.Files.APPLICATION_JAR;

    LOG.debug("Create and copy {}", Constants.Files.APPLICATION_JAR);
    Location location = locationCache.get(name, new LocationCache.Loader() {
      @Override
      public void load(String name, Location targetLocation) throws IOException {
        bundler.createBundle(targetLocation, classes);
      }
    });

    LOG.debug("Done {}", Constants.Files.APPLICATION_JAR);

    localFiles.put(Constants.Files.APPLICATION_JAR,
                   createLocalFile(Constants.Files.APPLICATION_JAR, location, true));
  }

  private void createResourcesJar(ApplicationBundler bundler, Map<String, LocalFile> localFiles,
                                  Path stagingDir) throws IOException {
    // If there is no resources, no need to create the jar file.
    if (resources.isEmpty()) {
      return;
    }

    LOG.debug("Create and copy {}", Constants.Files.RESOURCES_JAR);
    Location location = Locations.toLocation(new File(stagingDir.toFile(), Constants.Files.RESOURCES_JAR));
    bundler.createBundle(location, Collections.emptyList(), resources);
    LOG.debug("Done {}", Constants.Files.RESOURCES_JAR);
    localFiles.put(Constants.Files.RESOURCES_JAR, createLocalFile(Constants.Files.RESOURCES_JAR, location, true));
  }

  private void createRuntimeConfigJar(Path dir, Map<String, LocalFile> localFiles, Path stagingDir) throws IOException {
    LOG.debug("Create and copy {}", Constants.Files.RUNTIME_CONFIG_JAR);

    // Jar everything under the given directory, which contains different files needed by AM/runnable containers
    Location location = Locations.toLocation(Files.createTempFile(stagingDir,
                                                                  Constants.Files.RUNTIME_CONFIG_JAR, null));
    try (
      JarOutputStream jarOutput = new JarOutputStream(location.getOutputStream());
      DirectoryStream<Path> stream = Files.newDirectoryStream(dir)
    ) {
      for (Path path : stream) {
        JarEntry jarEntry = new JarEntry(path.getFileName().toString());
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
        jarEntry.setSize(attrs.size());
        jarEntry.setLastAccessTime(attrs.lastAccessTime());
        jarEntry.setLastModifiedTime(attrs.lastModifiedTime());
        jarOutput.putNextEntry(jarEntry);

        Files.copy(path, jarOutput);
        jarOutput.closeEntry();
      }
    }

    LOG.debug("Done {}", Constants.Files.RUNTIME_CONFIG_JAR);
    localFiles.put(Constants.Files.RUNTIME_CONFIG_JAR,
                   createLocalFile(Constants.Files.RUNTIME_CONFIG_JAR, location, true));
  }

  /**
   * Based on the given {@link TwillSpecification}, copy file to local filesystem.
   * @param spec The {@link TwillSpecification} for populating resource.
   */
  private Map<String, Collection<LocalFile>> populateRunnableLocalFiles(TwillSpecification spec,
                                                                        Path stagingDir) throws IOException {
    Map<String, Collection<LocalFile>> localFiles = new HashMap<>();

    LOG.debug("Populating Runnable LocalFiles");
    for (Map.Entry<String, RuntimeSpecification> entry : spec.getRunnables().entrySet()) {
      String runnableName = entry.getKey();

      for (LocalFile localFile : entry.getValue().getLocalFiles()) {
        LocalFile resolvedLocalFile = resolveLocalFile(localFile, stagingDir);
        localFiles.computeIfAbsent(runnableName, s -> new ArrayList<>()).add(resolvedLocalFile);
        LOG.debug("Added file {}", resolvedLocalFile.getURI());
      }
    }
    LOG.debug("Done Runnable LocalFiles");
    return localFiles;
  }

  private TwillRuntimeSpecification saveSpecification(TwillSpecification spec,
                                                      Path targetFile, Path stagingDir) throws IOException {
    final Map<String, Collection<LocalFile>> runnableLocalFiles = populateRunnableLocalFiles(spec, stagingDir);

    // Rewrite LocalFiles inside twillSpec
    Map<String, RuntimeSpecification> runtimeSpec = spec.getRunnables().entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> {
        RuntimeSpecification value = e.getValue();
        return new DefaultRuntimeSpecification(value.getName(), value.getRunnableSpecification(),
                                               value.getResourceSpecification(),
                                               runnableLocalFiles.getOrDefault(e.getKey(), Collections.emptyList()));
      }));

    // Serialize into a local temp file.
    LOG.debug("Creating {}", targetFile);
    try (Writer writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8)) {
      EventHandlerSpecification eventHandler = spec.getEventHandler();
      if (eventHandler == null) {
        eventHandler = new LogOnlyEventHandler().configure();
      }
      TwillSpecification newTwillSpec =
        new DefaultTwillSpecification(spec.getName(), runtimeSpec, spec.getOrders(),
                                      spec.getPlacementPolicies(), eventHandler);
      Map<String, String> configMap = Maps.newHashMap();
      for (Map.Entry<String, String> entry : hConf) {
        if (entry.getKey().startsWith("twill.")) {
          configMap.put(entry.getKey(), entry.getValue());
        }
      }

      TwillRuntimeSpecification twillRuntimeSpec = new TwillRuntimeSpecification(
        newTwillSpec, "", URI.create("."), "", RunIds.fromString(programRunId.getRun()), twillSpec.getName(),
        null, logLevels, maxRetries, configMap, runnableConfigs);
      TwillRuntimeSpecificationAdapter.create().toJson(twillRuntimeSpec, writer);
      LOG.debug("Done {}", targetFile);
      return twillRuntimeSpec;
    }
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

  private void saveClassPaths(Path targetDir) throws IOException {
    Files.write(targetDir.resolve(Constants.Files.APPLICATION_CLASSPATH),
                Joiner.on(':').join(applicationClassPaths).getBytes(StandardCharsets.UTF_8));
    Files.write(targetDir.resolve(Constants.Files.CLASSPATH),
                Joiner.on(':').join(classPaths).getBytes(StandardCharsets.UTF_8));
  }

  private void saveArguments(Arguments arguments, final Path targetPath) throws IOException {
    ArgumentsCodec.encode(arguments, () -> Files.newBufferedWriter(targetPath, StandardCharsets.UTF_8));
  }

  /**
   * Throws a {@link TimeoutException} if time passed since the given start time has exceeded the given timeout value.
   */
  private void throwIfTimeout(long startTime, long timeout, TimeUnit timeoutUnit) throws TimeoutException {
    long timeoutMillis = timeoutUnit.toMillis(timeout);
    if (System.currentTimeMillis() - startTime >= timeoutMillis) {
      throw new TimeoutException(String.format("Aborting startup of program run %s due to timeout after %d %s",
                                               programRunId, timeout, timeoutUnit.name().toLowerCase()));
    }
  }

  private LocalFile resolveLocalFile(LocalFile localFile, Path stagingDir) throws IOException {
    URI uri = localFile.getURI();
    String scheme = uri.getScheme();

    // If local file, resolve the last modified time and the file size
    if (scheme == null || "file".equals(scheme)) {
      File file = new File(uri.getPath());
      return new DefaultLocalFile(localFile.getName(), uri, file.lastModified(),
                                  file.length(), localFile.isArchive(), localFile.getPattern());
    }

    // If have the same scheme as the location factory, resolve time and size using Location
    if (Objects.equals(locationFactory.getHomeLocation().toURI().getScheme(), scheme)) {
      Location location = locationFactory.create(uri);
      return new DefaultLocalFile(localFile.getName(), uri, location.lastModified(),
                                  location.length(), localFile.isArchive(), localFile.getPattern());
    }

    // For other cases, attempt to save the URI content to local file, using support URLSteamHandler
    try (InputStream input = uri.toURL().openStream()) {
      Path tempFile = Files.createTempFile(stagingDir, localFile.getName(), Paths.getExtension(localFile.getName()));
      Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
      BasicFileAttributes attrs = Files.readAttributes(tempFile, BasicFileAttributes.class);
      return new DefaultLocalFile(localFile.getName(), tempFile.toUri(), attrs.lastModifiedTime().toMillis(),
                                  attrs.size(), localFile.isArchive(), localFile.getPattern());
    }
  }

  private void confirmRunnableName(String runnableName) {
    Preconditions.checkNotNull(runnableName);
    Preconditions.checkArgument(twillSpec.getRunnables().containsKey(runnableName),
                                "Runnable %s is not defined in the application.", runnableName);
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
      Preconditions.checkArgument(entry.getValue() != null,
                                  "Log level cannot be null for logger {}", entry.getKey());
      newLevels.put(entry.getKey(), entry.getValue().name());
    }
    this.logLevels.put(runnableName, newLevels);
  }

  /**
   * Returns the context ClassLoader if there is any, otherwise, returns ClassLoader of this class.
   */
  private ClassLoader getClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return classLoader == null ? getClass().getClassLoader() : classLoader;
  }

  private JvmOptions getJvmOptions() {
    // Append runnable specific extra options.
    Map<String, String> runnableExtraOptions = this.runnableExtraOptions.entrySet()
      .stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        e -> addClassLoaderClassName(extraOptions.isEmpty() ? e.getValue() : extraOptions + " " + e.getValue())));

    String globalOptions = addClassLoaderClassName(extraOptions);
    return new JvmOptions(globalOptions, runnableExtraOptions, debugOptions);
  }

  /**
   * Returns the extra options for the container JVM.
   */
  private String addClassLoaderClassName(String extraOptions) {
    if (classLoaderClassName == null) {
      return extraOptions;
    }
    String classLoaderProperty = "-D" + Constants.TWILL_CONTAINER_CLASSLOADER + "=" + classLoaderClassName;
    return extraOptions.isEmpty() ? classLoaderProperty : extraOptions + " " + classLoaderProperty;
  }

  /**
   * An interface for checking if a timeout time has already passed.
   */
  protected interface TimeoutChecker {
    void throwIfTimeout() throws TimeoutException;
  }
}
