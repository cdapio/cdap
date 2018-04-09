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

import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.internal.app.runtime.batch.dataproc.copied.YarnTwillController;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.Configs;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultLocalFile;
import org.apache.twill.internal.DefaultRuntimeSpecification;
import org.apache.twill.internal.DefaultTwillSpecification;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.JvmOptions;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.ProcessLauncher;
import org.apache.twill.internal.RunIds;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.appmaster.ApplicationMasterInfo;
import org.apache.twill.internal.appmaster.ApplicationMasterMain;
import org.apache.twill.internal.json.TwillRuntimeSpecificationAdapter;
import org.apache.twill.internal.utils.Resources;
import org.apache.twill.internal.yarn.VersionDetectYarnAppClientFactory;
import org.apache.twill.internal.yarn.YarnAppClient;
import org.apache.twill.internal.yarn.YarnApplicationReport;
import org.apache.twill.launcher.TwillLauncher;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DPLauncher {

  private final Configuration config;

  public DPLauncher(Configuration config) {
    this.config = config;
  }

  private Map<String, String> getLocalizeFiles() throws IOException {
    try (Reader reader = Files.newBufferedReader(java.nio.file.Paths.get(Constants.Files.LOCALIZE_FILES),
                                                 StandardCharsets.UTF_8)) {
      return new Gson().fromJson(reader, new TypeToken<Map<String, String>>() { }.getType());
    }
  }

  private TwillController doStart() throws IOException {
    File bootstrapDir = new File(System.getProperty("user.dir"));
    LocalLocationFactory locationFactory = new LocalLocationFactory(bootstrapDir);
    // TODO: unzip some jar files (at least the config one)



    Map<String, String> localizeFiles = getLocalizeFiles();
    System.out.println("localizeFiles: " + localizeFiles);

    File runtimeConfigJar = new File(localizeFiles.get(Constants.Files.RUNTIME_CONFIG_JAR));


    File destinationFolder = new File(Constants.Files.RUNTIME_CONFIG_JAR + "_unpacked");
    BundleJarUtil.unJar(com.google.common.io.Files.newInputStreamSupplier(runtimeConfigJar), destinationFolder);

    File twillSpecFile = new File(destinationFolder, Constants.Files.TWILL_SPEC);
    TwillRuntimeSpecification twillRuntimeSpec = TwillRuntimeSpecificationAdapter.create().fromJson(twillSpecFile);


    Map<String, Map<String, String>> runnableFiles;
    try (Reader reader = Files.newBufferedReader(java.nio.file.Paths.get("runnableFiles"),
                                                 StandardCharsets.UTF_8)) {
      runnableFiles = new Gson().fromJson(reader, new TypeToken<Map<String, Map<String, String>>>() { }.getType());
    }
    // TODO: publish to hdfs


    TwillSpecification spec = twillRuntimeSpec.getTwillSpecification();
    // Rewrite LocalFiles inside twillSpec
    Map<String, RuntimeSpecification> newRuntimeSpec = new HashMap<>();
    for (Map.Entry<String, RuntimeSpecification> entry : spec.getRunnables().entrySet()) {
      RuntimeSpecification value = entry.getValue();

      Collection<LocalFile> newLocalFiles = new ArrayList<>();
      for (LocalFile localFile : value.getLocalFiles()) {
        File runnableFile = new File(runnableFiles.get(entry.getKey()).get(localFile.getName()));
        newLocalFiles.add(new DefaultLocalFile(localFile.getName(), runnableFile.toURI(), runnableFile.lastModified(),
                                               runnableFile.length(), localFile.isArchive(), localFile.getPattern()));
      }
      newRuntimeSpec.put(entry.getKey(),
                         new DefaultRuntimeSpecification(value.getName(), value.getRunnableSpecification(),
                                                         value.getResourceSpecification(), newLocalFiles));
    }

    TwillSpecification newTwillSpec =
      new DefaultTwillSpecification(spec.getName(), newRuntimeSpec, spec.getOrders(),
                                    spec.getPlacementPolicies(), spec.getEventHandler());


    TwillRuntimeSpecification newTwillRuntimeSpec =
      new TwillRuntimeSpecification(newTwillSpec, twillRuntimeSpec.getFsUser(), twillRuntimeSpec.getTwillAppDir(),
                                    twillRuntimeSpec.getZkConnectStr(), twillRuntimeSpec.getTwillAppRunId(),
                                    twillRuntimeSpec.getTwillAppName(), twillRuntimeSpec.getRmSchedulerAddr(),
                                    twillRuntimeSpec.getLogLevels(), twillRuntimeSpec.getMaxRetries(),
                                    twillRuntimeSpec.getConfig(), twillRuntimeSpec.getRunnableConfigs());


    TwillRuntimeSpecificationAdapter.create().toJson(newTwillRuntimeSpec, twillSpecFile);
    // overwrite the zip which container the twill spec
    BundleJarUtil.createJar(destinationFolder, runtimeConfigJar);



    // TODO: get this from file. note: file may not always exist (according to comment in YarnTwillPreparer)
    // TODO: add classloader name
    String globalOptions = "";
    Map<String, String> runnableExtraOptions = new HashMap<>(); // TODO:
    JvmOptions.DebugOptions debugOptions = JvmOptions.DebugOptions.NO_DEBUG; // TODO:
    JvmOptions jvmOptions = new JvmOptions(globalOptions, runnableExtraOptions, debugOptions);


    Set<String> archives = new HashSet<>();
    archives.add(Constants.Files.TWILL_JAR);
    archives.add(Constants.Files.RUNTIME_CONFIG_JAR);
    archives.add(Constants.Files.APPLICATION_JAR);
    archives.add(Constants.Files.RESOURCES_JAR);
    // TODO: what else?


//    localizeFiles.put(Constants.Files.LOCALIZE_FILES, Constants.Files.LOCALIZE_FILES);
    final Map<String, LocalFile> localFiles = new HashMap<>();
    for (Map.Entry<String, String> stringStringEntry : localizeFiles.entrySet()) {
      File localizeFile = new File(stringStringEntry.getValue());
      localFiles.put(stringStringEntry.getKey(),
                     // TODO: do we ever use pattern?
                     new DefaultLocalFile(stringStringEntry.getKey(), localizeFile.toURI(), localizeFile.lastModified(),
                                          localizeFile.length(), archives.contains(stringStringEntry.getKey()), null)
      );
    }

    Location location = locationFactory.create(Constants.Files.LOCALIZE_FILES + "_map");
    System.out.println("exists: " + location.exists());
    System.out.println("delete: " + location.delete());
    // Serialize the list of LocalFiles, except the one we are generating here, as this file is used by AM only.
    // This file should never use LocationCache.
    try (Writer writer = new OutputStreamWriter(location.getOutputStream(), StandardCharsets.UTF_8)) {
      new Gson().toJson(localFiles.values(), writer);
    }
    localFiles.put(Constants.Files.LOCALIZE_FILES,
                   new DefaultLocalFile(Constants.Files.LOCALIZE_FILES, location.toURI(), location.lastModified(),
                                        location.length(), false, null));


    try {
      final YarnAppClient yarnAppClient = new VersionDetectYarnAppClientFactory().create(config);
      // TODO: schedulerQueue
      final ProcessLauncher<ApplicationMasterInfo> launcher =
        yarnAppClient.createLauncher(newTwillSpec, null);
      final ApplicationMasterInfo appMasterInfo = launcher.getContainerInfo();
      System.out.println("appMasterInfo: " + appMasterInfo);


      Callable<ProcessController<YarnApplicationReport>> submitTask =
        new Callable<ProcessController<YarnApplicationReport>>() {
          @Override
          public ProcessController<YarnApplicationReport> call() throws Exception {
            // Local files needed by AM
//            Map<String, LocalFile> localFiles = Maps.newHashMap();

            // creates and saves all the local files.
//            createLauncherJar(localFiles);
//            createTwillJar(createBundler(classAcceptor), yarnAppClient, localFiles);
//            createApplicationJar(createBundler(classAcceptor), localFiles);
//            createResourcesJar(createBundler(classAcceptor), localFiles);
//            create config jar...

//            createLocalizeFilesJson(localFiles);

//            LOG.debug("Submit AM container spec: {}", appMasterInfo);
            System.out.println("Submit AM container spec: " +  appMasterInfo);
            // java -Djava.io.tmpdir=tmp -cp launcher.jar:$HADOOP_CONF_DIR -XmxMemory
            //     org.apache.twill.internal.TwillLauncher
            //     appMaster.jar
            //     org.apache.twill.internal.appmaster.ApplicationMasterMain
            //     false
            int memory = Resources.computeMaxHeapSize(appMasterInfo.getMemoryMB(),
                                                      twillRuntimeSpec.getAMReservedMemory(),
                                                      twillRuntimeSpec.getAMMinHeapRatio());
            return launcher.prepareLaunch(ImmutableMap.of(), localFiles.values(),
                                          createSubmissionCredentials())
              .addCommand(
                "$JAVA_HOME/bin/java",
                "-Djava.io.tmpdir=tmp",
                "-Dyarn.appId=$" + EnvKeys.YARN_APP_ID_STR,
                "-Dtwill.app=$" + Constants.TWILL_APP_NAME,
                "-cp", Constants.Files.LAUNCHER_JAR + ":$HADOOP_CONF_DIR",
                "-Xmx" + memory + "m",
                jvmOptions.getAMExtraOptions(),
                TwillLauncher.class.getName(),
                ApplicationMasterMain.class.getName(),
                Boolean.FALSE.toString())
              .launch();
          }
        };

      boolean logCollectionEnabled = config.getBoolean(Configs.Keys.LOG_COLLECTION_ENABLED,
                                                       Configs.Defaults.LOG_COLLECTION_ENABLED);




      ZKClient zkClient = getZKClient(twillRuntimeSpec.getTwillAppName());
      Iterable<LogHandler> logHandlers = new ArrayList<>(); // TODO
      // timeout, timeoutUnits // TODO

      YarnTwillController controller =
        new YarnTwillController(twillRuntimeSpec.getTwillAppName(), twillRuntimeSpec.getTwillAppRunId(), zkClient,
                                logCollectionEnabled,
                                logHandlers, submitTask, Constants.APPLICATION_MAX_START_SECONDS, TimeUnit.SECONDS);
      controller.start();
      return controller;
    } catch (Exception e) {
//      LOG.error("Failed to submit application {}", twillSpec.getName(), e);
      throw Throwables.propagate(e);
    }
  }

  private ZKClient getZKClient(String appName) throws ExecutionException, InterruptedException {
    // String zkConnectStr =
    //   cConf.get(co.cask.cdap.common.conf.Constants.Zookeeper.QUORUM)
    //   + cConf.get(co.cask.cdap.common.conf.Constants.CFG_TWILL_ZK_NAMESPACE);

    String zkConnectStr = "localhost:2181/cdap";
    ZKClientService zkClientService = getZKClientService(zkConnectStr);
    zkClientService.startAndWait();

    // creating the /cdap node first. Attempting to directly create the /cdap/twill node fails.

    // Create the root node, so that the namespace root would get created if it is missing
    // If the exception is caused by node exists, then it's ok. Otherwise propagate the exception.
    ZKOperations.ignoreError(zkClientService.create("/", null, CreateMode.PERSISTENT),
                             KeeperException.NodeExistsException.class, null).get();

    ZKOperations.ignoreError(zkClientService.create("/twill", null, CreateMode.PERSISTENT),
                             KeeperException.NodeExistsException.class, null).get();

    return ZKClients.namespace(zkClientService, "/twill/" + appName);
  }

  private static final int ZK_TIMEOUT = 10000;

  private ZKClientService getZKClientService(String zkConnect) {
    return ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(ZKClientService.Builder.of(zkConnect)
                                   .setSessionTimeout(ZK_TIMEOUT)
                                   .build(), RetryStrategies.exponentialDelay(100, 2000, TimeUnit.MILLISECONDS))));
  }

  /**
   * Creates a {@link Credentials} for the application submission.
   */
  private Credentials createSubmissionCredentials() {
    /*
    Credentials credentials = new Credentials();
    try {
      // Acquires delegation token for the location
      List<Token<?>> tokens = YarnUtils.addDelegationTokens(config, appLocation.getLocationFactory(), credentials);
      if (LOG.isDebugEnabled()) {
        for (Token<?> token : tokens) {
          LOG.debug("Delegation token acquired for {}, {}", appLocation, token);
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to acquire delegation token for location {}", appLocation);
    }

    // Copy the user provided credentials.
    // It will override the location delegation tokens acquired above if user supplies it.
    credentials.addAll(this.credentials);
    return credentials;
    */
    return null;
  }

  public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
    Configuration config = new Configuration(new YarnConfiguration());
    System.out.println("RM Address: " + config.get(YarnConfiguration.RM_SCHEDULER_ADDRESS));

    TwillController twillController = new DPLauncher(config).doStart();
    System.out.println("sleeping...");
    TimeUnit.MINUTES.sleep(5);
    System.out.println("done sleeping!");

  }

}
