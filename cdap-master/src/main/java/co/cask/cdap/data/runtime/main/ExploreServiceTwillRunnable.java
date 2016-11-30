/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.security.RemoteUGIProvider;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.twill.AbstractMasterTwillRunnable;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.explore.executor.ExploreExecutorService;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.explore.service.hive.BaseHiveExploreService;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationEnforcementService;
import co.cask.cdap.security.authorization.RemotePrivilegesManager;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.twill.api.TwillContext;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Service for the Explore module that runs user queries in a Twill runnable.
 * It launches a discoverable HTTP servers, that execute SQL statements.
 */
public class ExploreServiceTwillRunnable extends AbstractMasterTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreServiceTwillRunnable.class);
  private static final Function<URL, String> URL_TO_PATH = new Function<URL, String>() {
    @Override
    public String apply(URL url) {
      return url.getPath();
    }
  };

  private Injector injector;

  public ExploreServiceTwillRunnable(String name, String cConfName, String hConfName) {
    super(name, cConfName, hConfName);
  }

  @Override
  protected void doInit(TwillContext context) {
    setupHive();

    CConfiguration cConf = getCConfiguration();
    Configuration hConf = getConfiguration();

    addResource(hConf, "yarn-site.xml");
    addResource(hConf, "mapred-site.xml");
    addResource(hConf, "hive-site.xml");
    addResource(hConf, "tez-site.xml");

    // Set the host name to the one provided by Twill
    cConf.set(Constants.Explore.SERVER_ADDRESS, context.getHost().getHostName());

    // NOTE: twill client will try to load all the classes present here - including hive classes but it
    // will fail since Hive classes are not in master classpath, and ignore those classes silently
    injector = createInjector(cConf, hConf);
    injector.getInstance(LogAppenderInitializer.class).initialize();

    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.EXPLORE_HTTP_USER_SERVICE));
  }

  @Override
  protected void getServices(List<? super Service> services) {
    services.add(injector.getInstance(ZKClientService.class));
    services.add(injector.getInstance(KafkaClientService.class));
    services.add(injector.getInstance(AuthorizationEnforcementService.class));
    services.add(injector.getInstance(ExploreExecutorService.class));
  }

  /**
   * Adds the given resource loaded from the current context classloader to the given {@link Configuration}.
   */
  private void addResource(Configuration hConf, String resource) {
    URL url = Thread.currentThread().getContextClassLoader().getResource(resource);
    if (url == null) {
      LOG.warn("{} could not be found as a resource.", resource);
    } else {
      LOG.info("Adding {} as configuration resource", url);
      hConf.addResource(url);
    }
  }

  /**
   * Returns the {@link URL} of the given resource loaded from the current context classloader if it is a local
   * resource. {@code null} will be returned if either resource is not found or is not a local resource.
   */
  @Nullable
  private URL getLocalResourceURL(String resource) {
    URL resourceURL = Thread.currentThread().getContextClassLoader().getResource(resource);
    if (resourceURL == null) {
      return null;
    }

    if (!"file".equals(resourceURL.getProtocol())) {
      return null;
    }
    return resourceURL;
  }

  /**
   * Returns the file name (the path after the last '/') of the given {@link URL}.
   */
  private String getFileName(URL url) {
    String path = url.getPath();
    int idx = path.lastIndexOf('/');
    return idx < 0 ? path : path.substring(idx + 1);
  }

  /**
   * Setup the environment needed by the embedded HiveServer2.
   */
  private void setupHive() {
    // Set local tmp dir to an absolute location in the twill runnable otherwise Hive complains
    File tmpDir = new File(System.getProperty("java.io.tmpdir")).getAbsoluteFile();
    File localScratchFile = new File(tmpDir, "hive-" + System.getProperty("user.name"));
    System.setProperty(HiveConf.ConfVars.LOCALSCRATCHDIR.toString(), localScratchFile.getAbsolutePath());
    LOG.info("Setting {} to {}", HiveConf.ConfVars.LOCALSCRATCHDIR.toString(),
             System.getProperty(HiveConf.ConfVars.LOCALSCRATCHDIR.toString()));

    ClassLoader classLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                   getClass().getClassLoader());

    // The current classloader should be a URLClassLoader, otherwise, nothing much we can do
    if (!(classLoader instanceof URLClassLoader)) {
      LOG.warn("Current ClassLoader is not an URLClassLoader {}." +
                 " No hive aux jars and *-site.xml classpath manipulation", classLoader);
      return;
    }

    // Filter out hive jars, only include non-hive jars (e.g. CDAP jars, hbase jars) from the container directory.
    // Basically those are the jars not directly under the container directory, since we localize hive jars to the
    // container directory. For non-hive jars, those get expanded by Twill under the $PWD/tmp directory, hence
    // they won't be directly under $PWD.
    // We also filter out jar with the same name and only taking the one that comes first in the current classloader
    // classpath, other the file localization for MR/Spark app launched by Hive will fail
    // For jars not in container directory, they are from the yarn application classpath (e.g. Hadoop jars), which
    // we don't need to include as well.
    List<URL> urls = Arrays.asList(((URLClassLoader) classLoader).getURLs());
    LOG.debug("Classloader urls: {}", urls);

    // Need to be a LinkedHashMap since we need to maintain the jar order
    Map<String, URL> hiveExtraJars = new LinkedHashMap<>();
    String userDir = System.getProperty("user.dir");
    for (URL url : urls) {
      String path = url.getPath();
      if (!path.endsWith(".jar") || !path.startsWith(userDir) || new File(path).getParent().equals(userDir)) {
        // This is hive jar, hence exclude it
        continue;
      }
      String fileName = getFileName(url);
      if (!hiveExtraJars.containsKey(fileName)) {
        hiveExtraJars.put(fileName, url);
      } else {
        LOG.info("Ignore jar with name {} that was added previously with {}", fileName, url);
      }
    }

    // Set the Hive aux jars property. This is for localizing jars needed for CDAP
    // These dependency files need to be copied over to hive job container.
    // The path are prefixed with "file:" in order to work with Hive started MR job.
    System.setProperty(HiveConf.ConfVars.HIVEAUXJARS.toString(),
                       Joiner.on(',').join(Iterables.transform(hiveExtraJars.values(), Functions.toStringFunction())));
    LOG.debug("Setting {} to {}", HiveConf.ConfVars.HIVEAUXJARS.toString(),
              System.getProperty(HiveConf.ConfVars.HIVEAUXJARS.toString()));

    // These dependency files need to be copied over to spark container
    System.setProperty(BaseHiveExploreService.SPARK_YARN_DIST_FILES,
                       Joiner.on(',').join(Iterables.transform(hiveExtraJars.values(), URL_TO_PATH)));
    LOG.debug("Setting {} to {}", BaseHiveExploreService.SPARK_YARN_DIST_FILES,
              System.getProperty(BaseHiveExploreService.SPARK_YARN_DIST_FILES));

    // Rewrite the yarn-site.xml, mapred-site.xml, hive-site.xml and tez-site.xml for classpath manipulation
    String extraClassPath = Joiner.on(',').join(
      Iterables.transform(hiveExtraJars.keySet(), new Function<String, String>() {
        @Override
        public String apply(String name) {
          return "$PWD/" + name;
        }
      }));
    extraClassPath += ",$PWD/*";

    rewriteConfigClasspath("yarn-site.xml", YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                           Joiner.on(",").join(YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH), extraClassPath);
    rewriteConfigClasspath("mapred-site.xml", MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
                           MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH, extraClassPath);
    rewriteConfigClasspath("tez-site.xml", TezConfiguration.TEZ_CLUSTER_ADDITIONAL_CLASSPATH_PREFIX, null,
                           extraClassPath);
    rewriteHiveConfig();

    // add hive-exec.jar to the HADOOP_CLASSPATH, which is used by the local mapreduce job launched by hive ,
    // we need to add this, otherwise when hive runs a MapRedLocalTask it cannot find
    // "org.apache.hadoop.hive.serde2.SerDe" class in its classpath.

    String hiveExecJar = new JobConf(org.apache.hadoop.hive.ql.exec.Task.class).getJar();
    Preconditions.checkNotNull(hiveExecJar, "Couldn't locate hive-exec.jar to be included in HADOOP_CLASSPATH " +
      "for MapReduce jobs launched by Hive");

    LOG.debug("Added hive-exec.jar {} to HADOOP_CLASSPATH to be included for MapReduce jobs launched by Hive",
              hiveExecJar);
    try {
      setupHadoopBin(Iterables.concat(hiveExtraJars.values(),
                                      Collections.singleton(new File(hiveExecJar).toURI().toURL())));
    } catch (IOException e) {
      LOG.error("Failed to generate hadoop binary to include hive-exec.jar.", e);
      throw Throwables.propagate(e);
    }
  }

  private void rewriteConfigClasspath(String resource, String key, @Nullable String defaultValue, String valuePrefix) {
    URL resourceURL = getLocalResourceURL(resource);
    if (resourceURL == null) {
      LOG.warn("Cannot find local resource {}. Configuration is not being modified.", resource);
      return;
    }

    try (InputStream is = new FileInputStream(new File(resourceURL.toURI()))) {
      Configuration conf = new Configuration(false);
      conf.addResource(is);
      String value = conf.get(key, defaultValue);
      value = (value == null) ? valuePrefix : valuePrefix + "," + value;

      LOG.debug("Setting {} to {} in {}", key, value, resourceURL);
      conf.set(key, value);

      File newConfFile = File.createTempFile(resource, ".tmp");
      try (FileOutputStream os = new FileOutputStream(newConfFile)) {
        conf.writeXml(os);
      }
      Files.move(newConfFile.toPath(), Paths.get(resourceURL.toURI()), StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      LOG.error("Failed to rewrite config file {}", resourceURL, e);
      throw Throwables.propagate(e);
    }
  }

  private void rewriteHiveConfig() {
    URL resourceURL = getLocalResourceURL("hive-site.xml");
    if (resourceURL == null) {
      LOG.warn("Cannot find local resource hive-site.xml. Configuration is not being modified.");
      return;
    }

    try (InputStream is = new FileInputStream(new File(resourceURL.toURI()))) {
      Configuration conf = new Configuration(false);
      conf.addResource(is);

      // we prefer jars at container's root directory before job.jar,
      // we edit the YARN_APPLICATION_CLASSPATH in yarn-site.xml and
      // setting the MAPREDUCE_JOB_CLASSLOADER and MAPREDUCE_JOB_USER_CLASSPATH_FIRST to false will put
      // YARN_APPLICATION_CLASSPATH before job.jar for container's classpath.
      conf.setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, false);
      conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, false);

      String sparkHome = System.getenv(Constants.SPARK_HOME);
      if (sparkHome != null) {
        LOG.debug("Setting spark.home in hive conf to {}", sparkHome);
        conf.set("spark.home", sparkHome);
      }

      File newConfFile = File.createTempFile("hive-site.xml", ".tmp");
      try (FileOutputStream os = new FileOutputStream(newConfFile)) {
        conf.writeXml(os);
      }
      Files.move(newConfFile.toPath(), Paths.get(resourceURL.toURI()), StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      LOG.error("Failed to rewrite config file {}", resourceURL, e);
      throw Throwables.propagate(e);
    }
  }

  private void setupHadoopBin(Iterable<URL> hiveAuxJars) throws IOException {
    HiveConf hiveConf = new HiveConf();
    String hadoopBin = hiveConf.get(HiveConf.ConfVars.HADOOPBIN.toString());

    // We over-ride HADOOPBIN setting in HiveConf to the script below, so that Hive uses this script to execute
    // map reduce jobs.
    // The below script updates HADOOP_CLASSPATH to contain hbase-protocol jar for RunJar commands,
    // so that the right version of protocol buffer jar gets loaded for HBase.
    // It also puts all the user jars, ie hive aux jars, in this classpath and in first position, so that
    // the right version of ASM jar gets loaded for Twill.
    // It then calls the real Hadoop bin with the same arguments.
    Path exploreHadoopBin = Files.createTempFile("explore.hadoop", ".bin",
                                         PosixFilePermissions.asFileAttribute(
                                           PosixFilePermissions.fromString("rwx------")));
    try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(exploreHadoopBin, StandardCharsets.UTF_8))) {
      writer.println("#!/usr/bin/env bash");
      writer.println("# This file is a hack to set HADOOP_CLASSPATH for Hive local mapreduce tasks.");
      writer.println("# This hack should go away when Twill supports setting of environmental variables for a ");
      writer.println("# TwillRunnable.");
      writer.println("if [ $# -ge 1 -a \"$1\" = \"jar\" ]; then");
      writer.print("  HADOOP_CLASSPATH=\"");
      Joiner.on(File.pathSeparatorChar).appendTo(writer, hiveAuxJars);
      writer.append(File.pathSeparatorChar).append("${HADOOP_CLASSPATH}\"").println();
      writer.println("  # Put user jars first in Hadoop classpath so that the ASM jar needed by Twill has");
      writer.println("  # the right version, and not the one provided with the Hadoop libs.");
      writer.println("  export HADOOP_USER_CLASSPATH_FIRST=true");
      writer.println("  export HADOOP_CLASSPATH");
      writer.println("  echo \"Explore modified HADOOP_CLASSPATH = $HADOOP_CLASSPATH\" 1>&2");
      writer.println("fi");
      writer.println();
      writer.append("exec ").append(hadoopBin).append(" \"$@\"").println();
    }

    LOG.info("Setting Hadoop bin to Explore Hadoop bin {}", exploreHadoopBin);
    System.setProperty(HiveConf.ConfVars.HADOOPBIN.toString(), exploreHadoopBin.toAbsolutePath().toString());
  }

  @VisibleForTesting
  static Injector createInjector(CConfiguration cConf, Configuration hConf) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(), new ZKClientModule(),
      new KafkaClientModule(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new LocationRuntimeModule().getDistributedModules(),
      new NamespaceClientRuntimeModule().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new LoggingModules().getDistributedModules(),
      new ExploreRuntimeModule().getDistributedModules(),
      new ExploreClientModule(),
      new ViewAdminModules().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NotificationFeedClientModule(),
      new AuditModule().getDistributedModules(),
      new AuthenticationContextModules().getMasterModule(),
      new SecureStoreModules().getDistributedModules(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(Store.class).to(DefaultStore.class);
          bind(UGIProvider.class).to(RemoteUGIProvider.class).in(Scopes.SINGLETON);
          bind(PrivilegesManager.class).to(RemotePrivilegesManager.class);
        }
      });
  }
}
