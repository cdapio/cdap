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

package io.cdap.cdap.data.runtime.main;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.twill.AbortOnTimeoutEventHandler;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import io.cdap.cdap.logging.LoggingUtil;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.TwillSpecification.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TwillApplication wrapper for Master Services running in YARN.
 */
public class MasterTwillApplication implements TwillApplication {

  private static final Logger LOG = LoggerFactory.getLogger(MasterTwillApplication.class);
  private static final String NAME = Constants.Service.MASTER_SERVICES;
  private static final String CCONF_NAME = "cConf.xml";
  private static final String HCONF_NAME = "hConf.xml";

  // A map from service name to cConf configuration key prefix for service specific container configuration
  // They are not necessarily the same (e.g. "transaction" vs "data.tx").
  // We follow the same naming prefix for configuring containers' memory/vcores.
  private static final Map<String, String> ALL_SERVICES = ImmutableMap.<String, String>builder()
      .put(Constants.Service.MESSAGING_SERVICE, "messaging.")
      .put(Constants.Service.TRANSACTION, "data.tx.")
      .put(Constants.Service.DATASET_EXECUTOR, "dataset.executor.")
      .put(Constants.Service.LOGSAVER, "log.saver.")
      .put(Constants.Service.METRICS_PROCESSOR, "metrics.processor.")
      .put(Constants.Service.METRICS, "metrics.")
      .build();

  private final CConfiguration cConf;

  private final Map<String, Integer> instanceCountMap;
  private final Map<String, Map<String, LocalizeResource>> runnableLocalizeResources;

  MasterTwillApplication(CConfiguration cConf, Map<String, Integer> instanceCountMap) {
    this.cConf = cConf;
    this.instanceCountMap = instanceCountMap;

    Map<String, Map<String, LocalizeResource>> runnableLocalizeResources = new HashMap<>();
    for (String service : ALL_SERVICES.keySet()) {
      runnableLocalizeResources.put(service, new HashMap<>());
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

    Path cConfPath = saveCConf(containerCConf, Files.createTempFile(tempDir, "cConf", ".xml"));
    Path hConfPath = saveHConf(hConf, Files.createTempFile(tempDir, "hConf", ".xml"));

    for (String service : ALL_SERVICES.keySet()) {
      Map<String, LocalizeResource> localizeResources = runnableLocalizeResources.get(service);
      localizeResources.put(CCONF_NAME, new LocalizeResource(cConfPath.toFile(), false));
      localizeResources.put(HCONF_NAME, new LocalizeResource(hConfPath.toFile(), false));
    }

    return extraClassPath;
  }

  /**
   * Returns a map from runnable name to configuration prefixes.
   */
  Map<String, String> getRunnableConfigPrefixes() {
    return ALL_SERVICES;
  }

  @Override
  public TwillSpecification configure() {
    // It is always present in cdap-default.xml
    final long noContainerTimeout = cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT,
        Long.MAX_VALUE);

    TwillSpecification.Builder.RunnableSetter runnableSetter =
        addMessaging(
            addDatasetOpExecutor(
                addLogSaverService(
                    addMetricsProcessor(
                        addMetricsService(
                            TwillSpecification.Builder.with().setName(NAME).withRunnable()
                        )
                    )
                )
            )
        );

    if (cConf.getBoolean(Constants.Transaction.TX_ENABLED)) {
      runnableSetter = addTransactionService(runnableSetter);
    }

    return runnableSetter
        .withOrder()
        .begin(Constants.Service.MESSAGING_SERVICE, Constants.Service.TRANSACTION,
            Constants.Service.DATASET_EXECUTOR)
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
    ResourceSpecification resourceSpec = createResourceSpecification(
        Constants.MetricsProcessor.NUM_CORES,
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
    ResourceSpecification resourceSpec = createResourceSpecification(
        Constants.Transaction.Container.NUM_CORES,
        Constants.Transaction.Container.MEMORY_MB,
        Constants.Service.TRANSACTION);
    return addResources(Constants.Service.TRANSACTION,
        builder.add(new TransactionServiceTwillRunnable(Constants.Service.TRANSACTION,
            CCONF_NAME, HCONF_NAME), resourceSpec));
  }

  private Builder.RunnableSetter addDatasetOpExecutor(Builder.MoreRunnable builder) {
    ResourceSpecification resourceSpec = createResourceSpecification(
        Constants.Dataset.Executor.CONTAINER_VIRTUAL_CORES,
        Constants.Dataset.Executor.CONTAINER_MEMORY_MB,
        Constants.Service.DATASET_EXECUTOR);
    return addResources(Constants.Service.DATASET_EXECUTOR,
        builder.add(new DatasetOpExecutorServerTwillRunnable(Constants.Service.DATASET_EXECUTOR,
            CCONF_NAME, HCONF_NAME), resourceSpec));
  }

  private Builder.RunnableSetter addMessaging(Builder.MoreRunnable builder) {
    ResourceSpecification resourceSpec = createResourceSpecification(
        Constants.MessagingSystem.CONTAINER_VIRTUAL_CORES,
        Constants.MessagingSystem.CONTAINER_MEMORY_MB,
        Constants.Service.MESSAGING_SERVICE);
    return addResources(Constants.Service.MESSAGING_SERVICE,
        builder.add(new MessagingServiceTwillRunnable(Constants.Service.MESSAGING_SERVICE,
            CCONF_NAME, HCONF_NAME), resourceSpec));
  }

  /**
   * Creates a {@link ResourceSpecification} based on the given configuration keys.
   */
  private ResourceSpecification createResourceSpecification(String vCoresKey, String memoryKey,
      String instancesKey) {
    int vCores = cConf.getInt(vCoresKey);
    int memory = cConf.getInt(memoryKey);
    int instances = instanceCountMap.get(instancesKey);

    Preconditions.checkArgument(vCores > 0, "Number of virtual cores must be > 0 for property %s",
        vCoresKey);
    Preconditions.checkArgument(memory > 0, "Memory size must be > 0 for property %s", memoryKey);
    Preconditions.checkArgument(instances > 0, "Number of instances must be > 0 for property %s",
        instancesKey);

    return ResourceSpecification.Builder.with()
        .setVirtualCores(vCores)
        .setMemory(memory, ResourceSpecification.SizeUnit.MEGA)
        .setInstances(instances)
        .build();
  }

  /**
   * Adds localize resources for the given service.
   */
  private Builder.RunnableSetter addResources(String service,
      Builder.RuntimeSpecificationAdder adder) {
    Map<String, LocalizeResource> localizeResources = runnableLocalizeResources.get(service);
    Iterator<Map.Entry<String, LocalizeResource>> iterator = localizeResources.entrySet()
        .iterator();

    TwillSpecification.Builder.MoreFile moreFile = null;
    while (iterator.hasNext()) {
      Map.Entry<String, LocalizeResource> entry = iterator.next();
      if (moreFile == null) {
        moreFile = adder.withLocalFiles()
            .add(entry.getKey(), entry.getValue().getURI(), entry.getValue().isArchive());
      } else {
        moreFile = moreFile.add(entry.getKey(), entry.getValue().getURI(),
            entry.getValue().isArchive());
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
    List<File> configFiles = DirUtils.listFiles(
        new File(cConf.get(Constants.Logging.PIPELINE_CONFIG_DIR)), "xml");
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
   * Prepares the {@link HBaseDDLExecutor} implementation for localization.
   */
  private void prepareHBaseDDLExecutorResources(Path tempDir, CConfiguration cConf)
      throws IOException {
    String ddlExecutorExtensionDir = cConf.get(Constants.HBaseDDLExecutor.EXTENSIONS_DIR);
    if (ddlExecutorExtensionDir == null) {
      // Nothing to localize
      return;
    }

    final File target = new File(tempDir.toFile(), "hbaseddlext.jar");
    BundleJarUtil.createJar(new File(ddlExecutorExtensionDir), target);
    for (String service : ALL_SERVICES.keySet()) {
      Map<String, LocalizeResource> localizeResourceMap = runnableLocalizeResources.get(service);
      localizeResourceMap.put(target.getName(), new LocalizeResource(target, true));
    }
    cConf.set(Constants.HBaseDDLExecutor.EXTENSIONS_DIR, target.getName());
  }
}
