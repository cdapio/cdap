/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.explore.guice;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.common.utils.FileUtils;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.explore.executor.ExploreExecutorHttpHandler;
import co.cask.cdap.explore.executor.ExploreExecutorService;
import co.cask.cdap.explore.executor.ExploreMetadataHttpHandler;
import co.cask.cdap.explore.executor.ExploreStatusHandler;
import co.cask.cdap.explore.executor.NamespacedExploreMetadataHttpHandler;
import co.cask.cdap.explore.executor.NamespacedQueryExecutorHttpHandler;
import co.cask.cdap.explore.executor.QueryExecutorHttpHandler;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.explore.service.ExploreServiceUtils;
import co.cask.cdap.explore.service.hive.BaseHiveExploreService;
import co.cask.cdap.explore.service.hive.Hive14ExploreService;
import co.cask.cdap.format.RecordFormats;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.cdap.hive.datasets.DatasetStorageHandler;
import co.cask.http.HttpHandler;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.inject.Exposed;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.twill.api.ClassAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Guice runtime module for the explore functionality.
 */
public class ExploreRuntimeModule extends RuntimeModule {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreRuntimeModule.class);

  @Override
  public Module getInMemoryModules() {
    // Turning off assertions for Hive packages, since some assertions in StandardStructObjectInspector do not work
    // when outer joins are run. It is okay to turn off Hive assertions since we assume Hive is a black-box that does
    // the right thing, and we only want to test our/our user's code.
    getClass().getClassLoader().setPackageAssertionStatus("org.apache.hadoop.hive", false);
    getClass().getClassLoader().setPackageAssertionStatus("org.apache.hive", false);
    return Modules.combine(new ExploreExecutorModule(), new ExploreLocalModule(true));
  }

  @Override
  public Module getStandaloneModules() {
    return Modules.combine(new ExploreExecutorModule(), new ExploreLocalModule(false));
  }

  @Override
  public Module getDistributedModules() {
    return Modules.combine(new ExploreExecutorModule(), new ExploreDistributedModule());
  }

  private static final class ExploreExecutorModule extends PrivateModule {

    @Override
    protected void configure() {
      Named exploreSeriveName = Names.named(Constants.Service.EXPLORE_HTTP_USER_SERVICE);
      Multibinder<HttpHandler> handlerBinder =
          Multibinder.newSetBinder(binder(), HttpHandler.class, exploreSeriveName);
      handlerBinder.addBinding().to(NamespacedQueryExecutorHttpHandler.class);
      handlerBinder.addBinding().to(QueryExecutorHttpHandler.class);
      handlerBinder.addBinding().to(NamespacedExploreMetadataHttpHandler.class);
      handlerBinder.addBinding().to(ExploreMetadataHttpHandler.class);
      handlerBinder.addBinding().to(ExploreExecutorHttpHandler.class);
      handlerBinder.addBinding().to(ExploreStatusHandler.class);
      CommonHandlers.add(handlerBinder);

      bind(ExploreExecutorService.class).in(Scopes.SINGLETON);
      expose(ExploreExecutorService.class);
    }
  }

  private static final class ExploreLocalModule extends PrivateModule {
    private final boolean isInMemory;

    ExploreLocalModule(boolean isInMemory) {
      this.isInMemory = isInMemory;
    }

    @Override
    protected void configure() {
      // Current version of hive used in standalone is Hive 14
      bind(ExploreService.class).annotatedWith(Names.named("explore.service.impl")).to(Hive14ExploreService.class);
      bind(ExploreService.class).toProvider(ExploreServiceProvider.class).in(Scopes.SINGLETON);
      expose(ExploreService.class);
      bind(boolean.class).annotatedWith(Names.named("explore.inmemory")).toInstance(isInMemory);

      bind(File.class).annotatedWith(Names.named(Constants.Explore.PREVIEWS_DIR_NAME))
        .toProvider(PreviewsDirProvider.class);

      bind(File.class).annotatedWith(Names.named(Constants.Explore.CREDENTIALS_DIR_NAME))
        .toProvider(CredentialsDirProvider.class);
    }

    private static final class PreviewsDirProvider implements Provider<File> {
      private final CConfiguration cConf;

      @Inject
      PreviewsDirProvider(CConfiguration cConf) {
        this.cConf = cConf;
      }

      @Override
      public File get() {
        return createLocalDir(cConf, "previewsDir");
      }
    }

    private static final class CredentialsDirProvider implements Provider<File> {
      private final CConfiguration cConf;

      @Inject
      CredentialsDirProvider(CConfiguration cConf) {
        this.cConf = cConf;
      }

      @Override
      public File get() {
        return createLocalDir(cConf, "credentialsDir");
      }
    }

    private static File createLocalDir(CConfiguration cConf, String dirName) {
      String localDirStr = cConf.get(Constants.Explore.LOCAL_DATA_DIR);
      File credentialsDir = new File(localDirStr, dirName);

      try {
        java.nio.file.Files.createDirectories(credentialsDir.toPath());
      } catch (IOException ioe) {
        // we have to wrap the IOException, because Provider#get doesn't declare it
        Throwables.propagate(ioe);
      }
      return credentialsDir;
    }

    @Singleton
    private static final class ExploreServiceProvider implements Provider<ExploreService> {
      private final CConfiguration cConf;
      private final Configuration hConf;
      private final ExploreService exploreService;
      private final boolean isInMemory;

      @Inject
      ExploreServiceProvider(CConfiguration cConf, Configuration hConf,
                             @Named("explore.service.impl") ExploreService exploreService,
                             @Named("explore.inmemory") boolean isInMemory) {
        this.exploreService = exploreService;
        this.cConf = cConf;
        this.hConf = hConf;
        this.isInMemory = isInMemory;
      }

      private static final long seed = System.currentTimeMillis();
      @Override
      public ExploreService get() {
        File hiveDataDir = new File(cConf.get(Constants.Explore.LOCAL_DATA_DIR));

        // The properties set using setProperty will be included to any new HiveConf object created,
        // at the condition that the configuration is known by Hive, and so is one of the HiveConf.ConfVars
        // variables.

        System.setProperty(HiveConf.ConfVars.SCRATCHDIR.toString(),
                           new File(hiveDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsolutePath());

        // Reset hadoop tmp dir because Hive does not pick it up from hConf
        System.setProperty("hadoop.tmp.dir", hConf.get("hadoop.tmp.dir"));

        File warehouseDir = new File(cConf.get(Constants.Explore.LOCAL_DATA_DIR), "warehouse");
        File databaseDir = new File(cConf.get(Constants.Explore.LOCAL_DATA_DIR), "database");

        if (isInMemory) {
          // This seed is required to make all tests pass when launched together, and when several of them
          // start a hive metastore / hive server.
          warehouseDir = new File(warehouseDir, Long.toString(seed));
          databaseDir = new File(databaseDir, Long.toString(seed));
        }

        LOG.debug("Setting {} to {}",
                  HiveConf.ConfVars.METASTOREWAREHOUSE.toString(), warehouseDir.getAbsoluteFile());
        System.setProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.toString(), warehouseDir.getAbsolutePath());

        // Set derby log location
        System.setProperty("derby.stream.error.file",
                           cConf.get(Constants.Explore.LOCAL_DATA_DIR) + File.separator + "derby.log");

        String connectUrl = String.format("jdbc:derby:;databaseName=%s;create=true", databaseDir.getAbsoluteFile());
        LOG.debug("Setting {} to {}", HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(), connectUrl);
        System.setProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(), connectUrl);

        // Some more local mode settings
        System.setProperty(HiveConf.ConfVars.LOCALMODEAUTO.toString(), "true");
        System.setProperty(HiveConf.ConfVars.SUBMITVIACHILD.toString(), "false");
        System.setProperty(MRConfig.FRAMEWORK_NAME, "local");

        // Disable security
        // Also need to disable security by making HiveAuthFactory.loginFromKeytab a no-op, since Hive >=0.14
        // ignores the HIVE_SERVER2_AUTHENTICATION property and instead uses UserGroupInformation.isSecurityEnabled()
        // (rewrite to HiveAuthFactory.loginFromKeytab bytecode is done in ExploreServiceUtils.traceDependencies)
        System.setProperty(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.toString(), "NONE");
        System.setProperty(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.toString(), "false");
        System.setProperty(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.toString(), "false");

        return exploreService;
      }
    }
  }

  private static final class ExploreDistributedModule extends PrivateModule {
    private static final Logger LOG = LoggerFactory.getLogger(ExploreDistributedModule.class);

    @Override
    protected void configure() {
      try {
        CConfiguration cConf = CConfiguration.create();
        File tmpDir = new File(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR)),
                               cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
        tmpDir.mkdirs();
        setupClasspath(tmpDir);

        // Set local tmp dir to an absolute location in the twill runnable otherwise Hive complains
        String localScratchPath = System.getProperty("java.io.tmpdir") + File.separator +
          "hive-" + System.getProperty("user.name");
        System.setProperty(HiveConf.ConfVars.LOCALSCRATCHDIR.toString(),
                           new File(localScratchPath).getAbsolutePath());
        LOG.info("Setting {} to {}", HiveConf.ConfVars.LOCALSCRATCHDIR.toString(),
                 System.getProperty(HiveConf.ConfVars.LOCALSCRATCHDIR.toString()));

        File previewDir = Files.createTempDir();
        LOG.info("Storing preview files in {}", previewDir.getAbsolutePath());
        bind(File.class).annotatedWith(Names.named(Constants.Explore.PREVIEWS_DIR_NAME)).toInstance(previewDir);

        File credentialsDir = Files.createTempDir();
        LOG.info("Storing credentials files in {}", credentialsDir.getAbsolutePath());
        bind(File.class).annotatedWith(Names.named(Constants.Explore.CREDENTIALS_DIR_NAME)).toInstance(credentialsDir);
      } catch (Throwable e) {
        throw Throwables.propagate(e);
      }
    }

    @Provides
    @Singleton
    @Exposed
    public ExploreService providesExploreService(Injector injector) {
      // Figure out which HiveExploreService class to load
      Class<? extends ExploreService> hiveExploreServiceCl = ExploreServiceUtils.getHiveService();
      LOG.info("Using Explore service class {}", hiveExploreServiceCl.getName());
      return injector.getInstance(hiveExploreServiceCl);
    }
  }

  private static void setupClasspath(File tmpDir) throws IOException {
    // Here we find the transitive dependencies and remove all paths that come from the boot class path -
    // those paths are not needed because the new JVM will have them in its boot class path.
    // It could even be wrong to keep them because in the target container, the boot class path may be different
    // (for example, if Hadoop uses a different Java version than CDAP).

    final Set<String> bootstrapClassPaths = ExploreServiceUtils.getBoostrapClasses();

    ClassAcceptor classAcceptor = new ClassAcceptor() {
       /* Excluding any class contained in the bootstrapClassPaths and Kryo classes and hive-exec.jar
        * We need to remove Kryo dependency in the Explore container. Spark introduced version 2.21 version of Kryo,
        * which would be normally shipped to the Explore container. Yet, Hive requires Kryo 2.22,
        * and gets it from the Hive jars - hive-exec.jar to be precise.
        * we also exclude hive jars as hive dependencies are found in job.jar.
        * */
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        if (bootstrapClassPaths.contains(classPathUrl.getFile()) ||
          className.startsWith("com.esotericsoftware.kryo") || classPathUrl.getFile().contains("hive")) {
          return false;
        }
        return true;
      }
    };

    Set<File> hBaseTableDeps = ExploreServiceUtils.traceDependencies(
      null, classAcceptor, tmpDir, HBaseTableUtilFactory.getHBaseTableUtilClass().getName());

    // Note the order of dependency jars is important so that HBase jars come first in the classpath order
    // LinkedHashSet maintains insertion order while removing duplicate entries.
    Set<File> orderedDependencies = new LinkedHashSet<>();
    orderedDependencies.addAll(hBaseTableDeps);
    orderedDependencies.addAll(ExploreServiceUtils.traceDependencies(null, classAcceptor, tmpDir,
                                                                     RemoteDatasetFramework.class.getName(),
                                                                     DatasetStorageHandler.class.getName(),
                                                                     RecordFormats.class.getName()));

    // Note: the class path entries need to be prefixed with "file://" for the jars to work when
    // Hive starts local map-reduce job.
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (File dep : orderedDependencies) {
      builder.add("file://" + dep.getAbsolutePath());
    }
    List<String> orderedDependenciesStr = builder.build();

    // These dependency files need to be copied over to spark container
    System.setProperty(BaseHiveExploreService.SPARK_YARN_DIST_FILES,
                       Joiner.on(',').join(Iterables.transform(orderedDependencies, new Function<File, String>() {
                         @Override
                         public String apply(File input) {
                           return input.getAbsolutePath();
                         }
                       })));
    LOG.debug("Setting {} to {}", BaseHiveExploreService.SPARK_YARN_DIST_FILES,
              System.getProperty(BaseHiveExploreService.SPARK_YARN_DIST_FILES));

    // These dependency files need to be copied over to hive job container
    System.setProperty(HiveConf.ConfVars.HIVEAUXJARS.toString(), Joiner.on(',').join(orderedDependenciesStr));
    LOG.debug("Setting {} to {}", HiveConf.ConfVars.HIVEAUXJARS.toString(),
              System.getProperty(HiveConf.ConfVars.HIVEAUXJARS.toString()));

    // add hive-exec.jar to the HADOOP_CLASSPATH, which is used by the local mapreduce job launched by hive ,
    // we need to add this, otherwise when hive runs a MapRedLocalTask it cannot find
    // "org.apache.hadoop.hive.serde2.SerDe" class in its classpath.
    List<String> orderedDependenciesWithHiveJar = Lists.newArrayList(orderedDependenciesStr);
    String hiveExecJar = new JobConf(org.apache.hadoop.hive.ql.exec.Task.class).getJar();
    Preconditions.checkNotNull(hiveExecJar, "Couldn't locate hive-exec.jar to be included in HADOOP_CLASSPATH " +
      "for MapReduce jobs launched by Hive");
    orderedDependenciesWithHiveJar.add(hiveExecJar);
    LOG.debug("Added hive-exec.jar {} to HADOOP_CLASSPATH to be included for MapReduce jobs launched by Hive",
              hiveExecJar);

    //TODO: Setup HADOOP_CLASSPATH hack, more info on why this is needed, see CDAP-9
    LocalMapreduceClasspathSetter classpathSetter =
      new LocalMapreduceClasspathSetter(new HiveConf(), tmpDir.getAbsolutePath(),
                                        orderedDependenciesWithHiveJar);
    for (File jar : hBaseTableDeps) {
      classpathSetter.accept(jar.getAbsolutePath());
    }
    classpathSetter.setupClasspathScript();
  }
}
