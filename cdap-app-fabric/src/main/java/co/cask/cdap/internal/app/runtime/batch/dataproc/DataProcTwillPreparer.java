/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataproc;

import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.FileUtils;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
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
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
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
import org.apache.twill.internal.json.LocalFileCodec;
import org.apache.twill.internal.json.TwillRuntimeSpecificationAdapter;
import org.apache.twill.internal.utils.Dependencies;
import org.apache.twill.internal.utils.Paths;
import org.apache.twill.internal.utils.Resources;
import org.apache.twill.internal.yarn.VersionDetectYarnAppClientFactory;
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
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
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
import javax.annotation.Nullable;

/**
 *
 */
public class DataProcTwillPreparer implements TwillPreparer {
  private static final Logger LOG = LoggerFactory.getLogger(DataProcTwillPreparer.class);
  private static final Function<Class<?>, String> CLASS_TO_NAME = new Function<Class<?>, String>() {
      public String apply(Class<?> cls) {
        return cls.getName();
      }
  };
  private final Configuration config;
  private final TwillSpecification twillSpec;
  private final Location appLocation;
  private final RunId runId;
  private final List<LogHandler> logHandlers = Lists.newArrayList();
  private final List<String> arguments = Lists.newArrayList();
  private final Set<Class<?>> dependencies = Sets.newIdentityHashSet();
  private final List<URI> resources = Lists.newArrayList();
  private final List<String> classPaths = Lists.newArrayList();
  private final ListMultimap<String, String> runnableArgs = ArrayListMultimap.create();
  private final Map<String, Map<String, String>> environments = Maps.newHashMap();
  private final List<String> applicationClassPaths = Lists.newArrayList();
  //private final Credentials credentials;
  private final Map<String, Map<String, String>> logLevels = Maps.newHashMap();
  private final LocationCache locationCache;
  private final Map<String, Integer> maxRetries = Maps.newHashMap();
  private final Map<String, Map<String, String>> runnableConfigs = Maps.newHashMap();
  private final Map<String, String> runnableExtraOptions = Maps.newHashMap();
  private final SSHConfig sshConfig;
  private final LocationFactory locationFactory;
  private String extraOptions;
  private JvmOptions.DebugOptions debugOptions;
  private String schedulerQueue;
  private ClassAcceptor classAcceptor;
  private String classLoaderClassName;

  DataProcTwillPreparer(Configuration config, TwillSpecification twillSpec, RunId runId,
                        Location appLocation, @Nullable String extraOptions, LocationCache locationCache,
                        SSHConfig sshConfig, LocationFactory locationFactory) {
    this.debugOptions = JvmOptions.DebugOptions.NO_DEBUG;
    this.config = config;
    this.twillSpec = twillSpec;
    this.runId = runId;
    this.appLocation = appLocation;
    this.extraOptions = extraOptions == null ? "" : extraOptions;
    this.classAcceptor = new ClassAcceptor();
    this.locationCache = locationCache;
    this.sshConfig = sshConfig;
    this.locationFactory = locationFactory;
  }

  private void confirmRunnableName(String runnableName) {
    Preconditions.checkNotNull(runnableName);
    Preconditions.checkArgument(twillSpec.getRunnables().containsKey(runnableName),
                                "Runnable %s is not defined in the application.", runnableName);
  }

  @Override
  public TwillPreparer withConfiguration(Map<String, String> config) {
    for (Map.Entry<String, String> entry : config.entrySet()) {
      this.config.set(entry.getKey(), entry.getValue());
    }
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
    for (String runnableName : runnables) {
      confirmRunnableName(runnableName);
    }
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
    try {
      // Local files needed by AM
      Map<String, LocalFile> localFiles = Maps.newHashMap();

      createLauncherJar(localFiles);
      createTwillJar(createBundler(classAcceptor), localFiles);
      createApplicationJar(createBundler(classAcceptor), localFiles);
      createResourcesJar(createBundler(classAcceptor), localFiles);

      TwillRuntimeSpecification twillRuntimeSpec;
      JvmOptions jvmOptions;
      Path runtimeConfigDir = Files.createTempDirectory(getLocalStagingDir().toPath(),
                                                        Constants.Files.RUNTIME_CONFIG_JAR);
      try {
        twillRuntimeSpec = saveSpecification(twillSpec, runtimeConfigDir.resolve(Constants.Files.TWILL_SPEC));
        saveLogback(runtimeConfigDir.resolve(Constants.Files.LOGBACK_TEMPLATE));
        saveClassPaths(runtimeConfigDir);
        jvmOptions = saveJvmOptions(runtimeConfigDir.resolve(Constants.Files.JVM_OPTIONS));
        saveArguments(new Arguments(arguments, runnableArgs),
                      runtimeConfigDir.resolve(Constants.Files.ARGUMENTS));
        saveEnvironments(runtimeConfigDir.resolve(Constants.Files.ENVIRONMENTS));
        createRuntimeConfigJar(runtimeConfigDir, localFiles);
      } finally {
        Paths.deleteRecursively(runtimeConfigDir);
      }




      // if it ends in a number, the directory will be scp'd ONTO
      // (directory will be replaced by a file and contents of the source file will be written onto that file)
      String remoteDir = "/tmp/remoteDir/dplauncher-" + System.currentTimeMillis();
      // TODO: sometimes we disconnect before the folder is created... fix this!
      System.out.println(SSHUtils.runCommand(sshConfig, "mkdir -p " + remoteDir));
      System.out.println(SSHUtils.runCommand(sshConfig, "ls -ltrh " + remoteDir));

      Map<String, String> remoteFiles = new HashMap<>();
      for (Map.Entry<String, LocalFile> localFileEntry : localFiles.entrySet()) {
        URI localFileURI = localFileEntry.getValue().getURI();
        String localFilePath = localFileURI.getPath();

        File localFile = new File(localFileURI);
        String remoteFilePath = remoteDir + "/" + localFile.getName();
        SSHUtils.scp(sshConfig, localFilePath, remoteFilePath);

        remoteFiles.put(localFileEntry.getKey(), remoteFilePath);
      }

      createLocalizeFilesJson(remoteFiles);
      SSHUtils.scp(sshConfig, remoteFiles.get(Constants.Files.LOCALIZE_FILES), remoteDir);


      Map<String, Map<String, String>> runnableFiles = new HashMap<>();
      for (Map.Entry<String, RuntimeSpecification> runnableEntry
        : twillRuntimeSpec.getTwillSpecification().getRunnables().entrySet()) {

        String runnableRemoteDir = remoteDir + "/" + runnableEntry.getKey() + System.currentTimeMillis();
        SSHUtils.runCommand(sshConfig, "mkdir -p " + runnableRemoteDir);
        Map<String, String> runnableFileMap = new HashMap<>();
        for (LocalFile lf : runnableEntry.getValue().getLocalFiles()) {
          File localFile = new File(lf.getURI());
          String remoteFilePath = runnableRemoteDir + "/" + localFile.getName();
          runnableFileMap.put(lf.getName(), remoteFilePath);
          SSHUtils.scp(sshConfig, localFile.getAbsolutePath(), remoteFilePath);
        }
        runnableFiles.put(runnableEntry.getKey(), runnableFileMap);
      }

      Location location = appLocation.append("runnableFiles");
      try (Writer writer = new OutputStreamWriter(location.getOutputStream(), StandardCharsets.UTF_8)) {
        new Gson().toJson(runnableFiles, writer);
      }
      LOG.debug("Done {}", Constants.Files.LOCALIZE_FILES);

      SSHUtils.scp(sshConfig, new File(location.toURI()).getAbsolutePath(), remoteDir + "/" + "runnableFiles");




      // TODO: how to get the error from this command: (its a type - nohub)
      // TODO: how to make this command blocking (And know that the command is done)
//      System.out.println(SSHUtils.runCommand(sshConfig, "nohub bash /home/aanwar/dp_launcher.sh " + remoteDir));
      System.out.println(SSHUtils.runCommand(sshConfig, "nohup bash /home/aanwar/dp_launcher.sh " + remoteDir));
      TimeUnit.SECONDS.sleep(5);
      // TODO: need to sleep? not seeing the yarn app run sometimes...
      return null;
    } catch (Exception e) {
      LOG.error("Failed to submit application {}", twillSpec.getName(), e);
      throw Throwables.propagate(e);
    }
  }


  /**
   * Returns the local staging directory based on the configuration.
   */
  private File getLocalStagingDir() {
    return new File(config.get(Configs.Keys.LOCAL_STAGING_DIRECTORY, Configs.Defaults.LOCAL_STAGING_DIRECTORY));
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

  private LocalFile createLocalFile(String name, Location location) throws IOException {
    return createLocalFile(name, location, false);
  }

  private LocalFile createLocalFile(String name, Location location, boolean archive) throws IOException {
    return new DefaultLocalFile(name, location.toURI(), location.lastModified(), location.length(), archive, null);
  }

  private void createTwillJar(final ApplicationBundler bundler,
                              Map<String, LocalFile> localFiles) throws IOException {
    LOG.debug("Create and copy {}", Constants.Files.TWILL_JAR);
    Location location = locationCache.get(Constants.Files.TWILL_JAR, new LocationCache.Loader() {
      @Override
      public void load(String name, Location targetLocation) throws IOException {
        //TODO: Stuck in the yarnAppClient class to make bundler being able to pickup the right yarn-client version?
        bundler.createBundle(targetLocation, ApplicationMasterMain.class,
                             TwillContainerMain.class, OptionSpec.class);
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
    bundler.createBundle(location, Collections.emptyList(), resources);
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
   * Based on the given {@link TwillSpecification}, copy file to local filesystem.
   * @param twillSpec The {@link TwillSpecification} for populating resource.
   */
  private Multimap<String, LocalFile> populateRunnableLocalFiles(TwillSpecification twillSpec) throws IOException {
    Multimap<String, LocalFile> localFiles = HashMultimap.create();
    LocalLocationFactory localLocationFactory = new LocalLocationFactory();

    LOG.debug("Populating Runnable LocalFiles");
    for (Map.Entry<String, RuntimeSpecification> entry: twillSpec.getRunnables().entrySet()) {
      String runnableName = entry.getKey();
      for (LocalFile localFile : entry.getValue().getLocalFiles()) {
        Location location;

        URI uri = localFile.getURI();
        if (uri.getScheme() == null || "file".equals(uri.getScheme())) {
          // If the source file location is already local file, no need to copy
          location = localLocationFactory.create(uri);
        } else {
          Location source = locationFactory.create(uri);
          LOG.debug("Create and copy {} : {}", runnableName, uri);
          // Preserves original suffix for expansion.
          location = localLocationFactory.create(localizeFile(source, localFile.getName()).toURI());
          LOG.debug("Done {} : {}", runnableName, uri);
        }

        LOG.info("runnable local file: {}", localFile.getURI());
        localFiles.put(runnableName,
                       new DefaultLocalFile(localFile.getName(), location.toURI(), location.lastModified(),
                                            location.length(), localFile.isArchive(), localFile.getPattern()));
      }
    }
    LOG.debug("Done Runnable LocalFiles");
    return localFiles;
  }

  private File localizeFile(Location sourceLocation, String target) throws IOException {
    // create a local file with restricted permissions
    // only allow the owner to read/write, since it contains credentials
    Path localKeytabFile = Files.createTempFile(getLocalStagingDir().toPath(), null, target);
    // copy to this local file

    LOG.info("Copying keytab file from {} to {}", sourceLocation, localKeytabFile);
    try (InputStream is = sourceLocation.getInputStream()) {
      Files.copy(is, localKeytabFile, StandardCopyOption.REPLACE_EXISTING);
    }

    return localKeytabFile.toFile();
  }

  private TwillRuntimeSpecification saveSpecification(TwillSpecification spec, Path targetFile) throws IOException {
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
      TwillSpecification newTwillSpec =
        new DefaultTwillSpecification(spec.getName(), runtimeSpec, spec.getOrders(),
                                      spec.getPlacementPolicies(), eventHandler);
      Map<String, String> configMap = Maps.newHashMap();
      for (Map.Entry<String, String> entry : config) {
        if (entry.getKey().startsWith("twill.")) {
          configMap.put(entry.getKey(), entry.getValue());
        }
      }

      TwillRuntimeSpecification twillRuntimeSpec = new TwillRuntimeSpecification(
        // fsuser may be inaccurate
              newTwillSpec, appLocation.getLocationFactory().getHomeLocation().getName(),
        // appLocation may be inaccurate
              appLocation.toURI(), "localhost:2181", runId, twillSpec.getName(),
              null,
              logLevels, maxRetries, configMap, runnableConfigs);
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

  private JvmOptions saveJvmOptions(final Path targetPath) throws IOException {
    // Append runnable specific extra options.
    Map<String, String> runnableExtraOptions = Maps.newHashMap(
            Maps.transformValues(this.runnableExtraOptions, new Function<String, String>() {
                @Override
                public String apply(String options) {
                  return addClassLoaderClassName(extraOptions.isEmpty() ? options : extraOptions + " " + options);
                }
            }));

    String globalOptions = addClassLoaderClassName(extraOptions);
    JvmOptions jvmOptions = new JvmOptions(globalOptions, runnableExtraOptions, debugOptions);
    if (globalOptions.isEmpty() && runnableExtraOptions.isEmpty()
            && JvmOptions.DebugOptions.NO_DEBUG.equals(debugOptions)) {
      // If no vm options, no need to localize the file.
      return jvmOptions;
    }

    LOG.debug("Creating {}", targetPath);
    try (Writer writer = Files.newBufferedWriter(targetPath, StandardCharsets.UTF_8)) {
      new Gson().toJson(new JvmOptions(globalOptions, runnableExtraOptions, debugOptions), writer);
    }
    LOG.debug("Done {}", targetPath);
    return jvmOptions;
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
  private void createLocalizeFilesJson(Map<String, String> files) throws IOException {
    LOG.debug("Create and copy {}", Constants.Files.LOCALIZE_FILES);
    Location location = appLocation.append(Constants.Files.LOCALIZE_FILES);

    // Serialize the list of LocalFiles, except the one we are generating here, as this file is used by AM only.
    // This file should never use LocationCache.
    try (Writer writer = new OutputStreamWriter(location.getOutputStream(), StandardCharsets.UTF_8)) {
      new Gson().toJson(files, writer);
    }
    LOG.debug("Done {}", Constants.Files.LOCALIZE_FILES);
    files.put(Constants.Files.LOCALIZE_FILES,
                   createLocalFile(Constants.Files.LOCALIZE_FILES, location).getURI().getPath());
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
    return new ApplicationBundler(classAcceptor).setTempDir(getLocalStagingDir());
  }

}
