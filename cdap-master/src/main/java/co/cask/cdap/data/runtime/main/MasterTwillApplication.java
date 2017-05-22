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

package co.cask.cdap.data.runtime.main;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.twill.AbortOnTimeoutEventHandler;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.explore.service.ExploreServiceUtils;
import co.cask.cdap.hive.ExploreUtils;
import co.cask.cdap.internal.app.runtime.batch.distributed.MapReduceContainerHelper;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.logging.LoggingUtil;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.TwillSpecification.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * TwillApplication wrapper for Master Services running in YARN.
 */
public class MasterTwillApplication implements TwillApplication {
  private static final Logger LOG = LoggerFactory.getLogger(MasterTwillApplication.class);
  private static final String NAME = Constants.Service.MASTER_SERVICES;
  private static final String CCONF_NAME = "cConf.xml";
  private static final String HCONF_NAME = "hConf.xml";
  private static final Set<String> ALL_SERVICES = ImmutableSet.of(
    Constants.Service.MESSAGING_SERVICE,
    Constants.Service.TRANSACTION,
    Constants.Service.DATASET_EXECUTOR,
    Constants.Service.STREAMS,
    Constants.Service.LOGSAVER,
    Constants.Service.METRICS_PROCESSOR,
    Constants.Service.METRICS,
    Constants.Service.EXPLORE_HTTP_USER_SERVICE
  );

  private final CConfiguration cConf;

  private final Map<String, Integer> instanceCountMap;
  private final Map<String, Map<String, LocalizeResource>> runnableLocalizeResources;

  MasterTwillApplication(CConfiguration cConf, Map<String, Integer> instanceCountMap) {
    this.cConf = cConf;
    this.instanceCountMap = instanceCountMap;

    Map<String, Map<String, LocalizeResource>> runnableLocalizeResources = new HashMap<>();
    for (String service : ALL_SERVICES) {
      runnableLocalizeResources.put(service, new HashMap<String, LocalizeResource>());
    }
    this.runnableLocalizeResources = runnableLocalizeResources;
  }

  /**
   * Prepares the resources that need to be localized to service containers.
   *
   * @param tempDir a temporary directory for creating files to be localized
   * @param hConf the hadoop configuration
   * @return a list of extra classpath that need to be added to each container.
   * @throws IOException if failed to prepare localize resources
   */
  List<String> prepareLocalizeResource(Path tempDir, Configuration hConf) throws IOException {
    CConfiguration containerCConf = CConfiguration.copy(cConf);
    containerCConf.set(Constants.CFG_LOCAL_DATA_DIR, "data");

    List<String> extraClassPath = new ArrayList<>();

    prepareLogSaverResources(tempDir, containerCConf,
                             runnableLocalizeResources.get(Constants.Service.LOGSAVER), extraClassPath);
    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      prepareExploreResources(tempDir, hConf,
                              runnableLocalizeResources.get(Constants.Service.EXPLORE_HTTP_USER_SERVICE),
                              extraClassPath);
    }

    Path cConfPath = saveCConf(containerCConf, Files.createTempFile(tempDir, "cConf", ".xml"));
    Path hConfPath = saveHConf(hConf, Files.createTempFile(tempDir, "hConf", ".xml"));

    for (String service : ALL_SERVICES) {
      Map<String, LocalizeResource> localizeResources = runnableLocalizeResources.get(service);
      localizeResources.put(CCONF_NAME, new LocalizeResource(cConfPath.toFile(), false));
      localizeResources.put(HCONF_NAME, new LocalizeResource(hConfPath.toFile(), false));
    }

    return extraClassPath;
  }

  @Override
  public TwillSpecification configure() {
    // It is always present in cdap-default.xml
    final long noContainerTimeout = cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE);

    TwillSpecification.Builder.RunnableSetter runnableSetter =
      addMessaging(
        addDatasetOpExecutor(
          addLogSaverService(
            addStreamService(
              addTransactionService(
                addMetricsProcessor (
                  addMetricsService(
                    TwillSpecification.Builder.with().setName(NAME).withRunnable()
                  )
                )
              )
            )
          )
        )
      );

    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      LOG.info("Adding explore runnable.");
      runnableSetter = addExploreService(runnableSetter);
    } else {
      LOG.info("Explore module disabled - will not launch explore runnable.");
    }
    return runnableSetter
        .withPlacementPolicy()
          .add(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, Constants.Service.STREAMS)
        .withOrder()
          .begin(Constants.Service.MESSAGING_SERVICE, Constants.Service.TRANSACTION, Constants.Service.DATASET_EXECUTOR)
        .withEventHandler(new AbortOnTimeoutEventHandler(noContainerTimeout))
        .build();
  }

  private Builder.RunnableSetter addLogSaverService(Builder.MoreRunnable builder) {
    ResourceSpecification resourceSpec = createResourceSpecification(Constants.LogSaver.NUM_CORES,
                                                                     Constants.LogSaver.MEMORY_MB,
                                                                     Constants.Service.LOGSAVER);
    return addResources(Constants.Service.LOGSAVER,
                        builder.add(new LogSaverTwillRunnable(Constants.Service.LOGSAVER,
                                                              CCONF_NAME, HCONF_NAME), resourceSpec));
  }

  private Builder.RunnableSetter addMetricsProcessor(Builder.MoreRunnable builder) {
    ResourceSpecification resourceSpec = createResourceSpecification(Constants.MetricsProcessor.NUM_CORES,
                                                                     Constants.MetricsProcessor.MEMORY_MB,
                                                                     Constants.Service.METRICS_PROCESSOR);
    return addResources(Constants.Service.METRICS_PROCESSOR,
                        builder.add(new MetricsProcessorTwillRunnable(Constants.Service.METRICS_PROCESSOR,
                                                                      CCONF_NAME, HCONF_NAME), resourceSpec));
  }

  private Builder.RunnableSetter addMetricsService(Builder.MoreRunnable builder) {
    ResourceSpecification resourceSpec = createResourceSpecification(Constants.Metrics.NUM_CORES,
                                                                     Constants.Metrics.MEMORY_MB,
                                                                     Constants.Service.METRICS);
    return addResources(Constants.Service.METRICS,
                        builder.add(new MetricsTwillRunnable(Constants.Service.METRICS,
                                                             CCONF_NAME, HCONF_NAME), resourceSpec));
  }

  private Builder.RunnableSetter addTransactionService(Builder.MoreRunnable builder) {
    ResourceSpecification resourceSpec = createResourceSpecification(Constants.Transaction.Container.NUM_CORES,
                                                                     Constants.Transaction.Container.MEMORY_MB,
                                                                     Constants.Service.TRANSACTION);
    return addResources(Constants.Service.TRANSACTION,
                        builder.add(new TransactionServiceTwillRunnable(Constants.Service.TRANSACTION,
                                                                        CCONF_NAME, HCONF_NAME), resourceSpec));
  }

  private Builder.RunnableSetter addStreamService(Builder.MoreRunnable builder) {
    ResourceSpecification resourceSpec = createResourceSpecification(Constants.Stream.CONTAINER_VIRTUAL_CORES,
                                                                     Constants.Stream.CONTAINER_MEMORY_MB,
                                                                     Constants.Service.STREAMS);
    return addResources(Constants.Service.STREAMS,
                        builder.add(new StreamHandlerRunnable(Constants.Service.STREAMS,
                                                              CCONF_NAME, HCONF_NAME), resourceSpec));
  }

  private Builder.RunnableSetter addDatasetOpExecutor(Builder.MoreRunnable builder) {
    ResourceSpecification resourceSpec = createResourceSpecification(Constants.Dataset.Executor.CONTAINER_VIRTUAL_CORES,
                                                                     Constants.Dataset.Executor.CONTAINER_MEMORY_MB,
                                                                     Constants.Service.DATASET_EXECUTOR);
    return addResources(Constants.Service.DATASET_EXECUTOR,
                        builder.add(new DatasetOpExecutorServerTwillRunnable(Constants.Service.DATASET_EXECUTOR,
                                                                             CCONF_NAME, HCONF_NAME), resourceSpec));
  }

  private Builder.RunnableSetter addExploreService(Builder.MoreRunnable builder) {
    ResourceSpecification resourceSpec = createResourceSpecification(Constants.Explore.CONTAINER_VIRTUAL_CORES,
                                                                     Constants.Explore.CONTAINER_MEMORY_MB,
                                                                     Constants.Service.EXPLORE_HTTP_USER_SERVICE);
    return addResources(Constants.Service.EXPLORE_HTTP_USER_SERVICE,
                        builder.add(new ExploreCustomClassLoaderTwillRunnable(
                          new ExploreServiceTwillRunnable(Constants.Service.EXPLORE_HTTP_USER_SERVICE,
                                                          CCONF_NAME, HCONF_NAME).configure()), resourceSpec));
  }

  private Builder.RunnableSetter addMessaging(Builder.MoreRunnable builder) {
    ResourceSpecification resourceSpec = createResourceSpecification(Constants.MessagingSystem.CONTAINER_VIRTUAL_CORES,
                                                                     Constants.MessagingSystem.CONTAINER_MEMORY_MB,
                                                                     Constants.Service.MESSAGING_SERVICE);
    return addResources(Constants.Service.MESSAGING_SERVICE,
                        builder.add(new MessagingServiceTwillRunnable(Constants.Service.MESSAGING_SERVICE,
                                                                      CCONF_NAME, HCONF_NAME), resourceSpec));
  }

  /**
   * Creates a {@link ResourceSpecification} based on the given configuration keys.
   */
  private ResourceSpecification createResourceSpecification(String vCoresKey, String memoryKey, String instancesKey) {
    int vCores = cConf.getInt(vCoresKey);
    int memory = cConf.getInt(memoryKey);
    int instances = instanceCountMap.get(instancesKey);

    Preconditions.checkArgument(vCores > 0, "Number of virtual cores must be > 0 for property %s", vCoresKey);
    Preconditions.checkArgument(memory > 0, "Memory size must be > 0 for property %s", memoryKey);
    Preconditions.checkArgument(instances > 0, "Number of instances must be > 0 for property %s", instancesKey);

    return ResourceSpecification.Builder.with()
      .setVirtualCores(vCores)
      .setMemory(memory, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();
  }

  /**
   * Adds localize resources for the given service.
   */
  private Builder.RunnableSetter addResources(String service, Builder.RuntimeSpecificationAdder adder) {
    Map<String, LocalizeResource> localizeResources = runnableLocalizeResources.get(service);
    Iterator<Map.Entry<String, LocalizeResource>> iterator = localizeResources.entrySet().iterator();

    TwillSpecification.Builder.MoreFile moreFile = null;
    while (iterator.hasNext()) {
      Map.Entry<String, LocalizeResource> entry = iterator.next();
      if (moreFile == null) {
        moreFile = adder.withLocalFiles().add(entry.getKey(), entry.getValue().getURI(), entry.getValue().isArchive());
      } else {
        moreFile = moreFile.add(entry.getKey(), entry.getValue().getURI(), entry.getValue().isArchive());
      }
    }
    return moreFile == null ? adder.noLocalFiles() : moreFile.apply();
  }

  /**
   * Serializes the given {@link CConfiguration} to the give file.
   */
  private Path saveCConf(CConfiguration cConf, Path file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file, Charsets.UTF_8)) {
      cConf.writeXml(writer);
    }
    return file;
  }

  /**
   * Serializes the given {@link Configuration} to the give file.
   */
  private Path saveHConf(Configuration conf, Path file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file, Charsets.UTF_8)) {
      conf.writeXml(writer);
    }
    return file;
  }

  /**
   * Prepares resources that need to be localized to the log saver container.
   */
  private void prepareLogSaverResources(Path tempDir, CConfiguration containerCConf,
                                        Map<String, LocalizeResource> localizeResources,
                                        Collection<String> extraClassPath) throws IOException {
    String configJarName = "log.config.jar";
    String libJarName = "log.lib.jar";

    // Localize log config files
    List<File> configFiles = DirUtils.listFiles(new File(cConf.get(Constants.Logging.PIPELINE_CONFIG_DIR)), "xml");
    if (!configFiles.isEmpty()) {
      Path configJar = Files.createTempFile(tempDir, "log.config", ".jar");
      try (JarOutputStream jarOutput = new JarOutputStream(Files.newOutputStream(configJar))) {
        for (File configFile : configFiles) {
          jarOutput.putNextEntry(new JarEntry(configFile.getName()));
          Files.copy(configFile.toPath(), jarOutput);
          jarOutput.closeEntry();
        }
      }
      localizeResources.put(configJarName, new LocalizeResource(configJar.toUri(), true));
    }
    // It's ok to set to a non-existing directory in case there is no config files
    containerCConf.set(Constants.Logging.PIPELINE_CONFIG_DIR, configJarName);

    // Localize log lib jars
    // First collect jars under each of the configured lib directory
    List<File> libJars = LoggingUtil.getExtensionJars(cConf);
    if (!libJars.isEmpty()) {
      Path libJar = Files.createTempFile("log.lib", ".jar");
      try (JarOutputStream jarOutput = new JarOutputStream(Files.newOutputStream(libJar))) {
        for (File jarFile : libJars) {
          jarOutput.putNextEntry(new JarEntry(jarFile.getName()));
          Files.copy(jarFile.toPath(), jarOutput);
          jarOutput.closeEntry();

          // Add the log lib jar to the container classpath
          extraClassPath.add(libJarName + File.separator + jarFile.getName());
        }
      }
      localizeResources.put(libJarName, new LocalizeResource(libJar.toUri(), true));
    }
    // Set it to empty value since we don't use this in the container.
    // All jars are already added as part of container classpath.
    containerCConf.set(Constants.Logging.PIPELINE_LIBRARY_DIR, "");
  }

  /**
   * Prepares resources to be localized to the explore container.
   */
  private void prepareExploreResources(Path tempDir, Configuration hConf,
                                       Map<String, LocalizeResource> localizeResources,
                                       Collection<String> extraClassPath) throws IOException {
    // Find the jars in the yarn application classpath
    String yarnAppClassPath = Joiner.on(File.pathSeparatorChar).join(
      hConf.getTrimmedStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                              YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));
    final Set<File> yarnAppJarFiles = new LinkedHashSet<>();
    Iterables.addAll(yarnAppJarFiles, ExploreUtils.getClasspathJarFiles(yarnAppClassPath));


    // Filter out jar files that are already in the yarn application classpath as those,
    // are already available in the Explore container.
    Iterable<File> exploreFiles = Iterables.filter(
      ExploreUtils.getExploreClasspathJarFiles("tgz", "gz"), new Predicate<File>() {
        @Override
        public boolean apply(File file) {
          return !yarnAppJarFiles.contains(file);
        }
      });

    // Create a zip file that contains all explore jar files.
    // Upload and localizing one big file is fast than many small one.
    String exploreArchiveName = "explore.archive.zip";
    Path exploreArchive = Files.createTempFile(tempDir, "explore.archive", ".zip");
    Set<String> addedJar = new HashSet<>();

    try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(exploreArchive))) {
      zos.setLevel(Deflater.NO_COMPRESSION);

      for (File file : exploreFiles) {
        if (file.getName().endsWith(".tgz") || file.getName().endsWith(".gz")) {
          // It's an archive, hence localize it archive so that it will be expanded to a directory on the container
          localizeResources.put(file.getName(), new LocalizeResource(file, true));
          // Includes the expanded directory, jars under that directory and jars under the "lib" to classpath
          extraClassPath.add(file.getName());
          extraClassPath.add(file.getName() + "/*");
          extraClassPath.add(file.getName() + "/lib/*");
        } else {
          // For jar file, add it to explore archive
          File targetFile = tempDir.resolve(System.currentTimeMillis() + "-" + file.getName()).toFile();
          File resultFile = ExploreServiceUtils.patchHiveClasses(file, targetFile);
          if (resultFile == targetFile) {
            LOG.info("Rewritten HiveAuthFactory from jar file {} to jar file {}", file, resultFile);
          }

          // don't add duplicate jar
          if (addedJar.add(resultFile.getName())) {
            zos.putNextEntry(new ZipEntry(resultFile.getName()));
            Files.copy(resultFile.toPath(), zos);
            extraClassPath.add(exploreArchiveName + File.separator + resultFile.getName());
          }
        }
      }
    }

    if (!addedJar.isEmpty()) {
      localizeResources.put(exploreArchiveName, new LocalizeResource(exploreArchive.toFile(), true));
    }

    // Explore also depends on MR, hence adding MR jars to the classpath.
    // Depending on how the cluster is configured, we might need to localize the MR framework tgz as well.
    MapReduceContainerHelper.localizeFramework(hConf, localizeResources);
    MapReduceContainerHelper.addMapReduceClassPath(hConf, extraClassPath);
    LOG.trace("Jars in extra classpath after adding jars in explore classpath: {}", extraClassPath);
  }
}
