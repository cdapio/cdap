package com.continuuity.explore.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.data2.datafabric.dataset.RemoteDatasetFramework;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.explore.executor.ExploreExecutorHttpHandler;
import com.continuuity.explore.executor.ExploreExecutorService;
import com.continuuity.explore.executor.ExplorePingHandler;
import com.continuuity.explore.executor.QueryExecutorHttpHandler;
import com.continuuity.explore.service.ExploreService;
import com.continuuity.explore.service.ExploreServiceUtils;
import com.continuuity.explore.service.hive.Hive13ExploreService;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.hive.datasets.DatasetStorageHandler;
import com.continuuity.http.HttpHandler;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
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
import org.apache.hadoop.mapreduce.MRConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
  public Module getSingleNodeModules() {
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
      handlerBinder.addBinding().to(QueryExecutorHttpHandler.class);
      handlerBinder.addBinding().to(ExploreExecutorHttpHandler.class);
      handlerBinder.addBinding().to(ExplorePingHandler.class);
      handlerBinder.addBinding().to(PingHandler.class);

      bind(ExploreExecutorService.class).in(Scopes.SINGLETON);
      expose(ExploreExecutorService.class);
    }
  }

  private static final class ExploreLocalModule extends PrivateModule {
    private final boolean isInMemory;

    public ExploreLocalModule(boolean isInMemory) {
      this.isInMemory = isInMemory;
    }

    @Override
    protected void configure() {
      // Current version of hive used in Singlenode is Hive 13
      bind(ExploreService.class).annotatedWith(Names.named("explore.service.impl")).to(Hive13ExploreService.class);
      bind(ExploreService.class).toProvider(ExploreServiceProvider.class).in(Scopes.SINGLETON);
      expose(ExploreService.class);

      bind(boolean.class).annotatedWith(Names.named("explore.inmemory")).toInstance(isInMemory);
    }

    @Singleton
    private static final class ExploreServiceProvider implements Provider<ExploreService> {

      private final CConfiguration cConf;
      private final ExploreService exploreService;
      private final boolean isInMemory;

      @Inject
      public ExploreServiceProvider(CConfiguration cConf,
                                    @Named("explore.service.impl") ExploreService exploreService,
                                    @Named("explore.inmemory") boolean isInMemory) {
        this.exploreService = exploreService;
        this.cConf = cConf;
        this.isInMemory = isInMemory;
      }

      private static final long seed = System.currentTimeMillis();
      @Override
      public ExploreService get() {
        File warehouseDir = new File(cConf.get(Constants.Explore.CFG_LOCAL_DATA_DIR), "warehouse");
        File databaseDir = new File(cConf.get(Constants.Explore.CFG_LOCAL_DATA_DIR), "database");

        if (isInMemory) {
          // This seed is required to make all tests pass when launched together, and when several of them
          // start a hive metastore / hive server.
          // TODO try to remove once maven is there - REACTOR-267
          warehouseDir = new File(warehouseDir, Long.toString(seed));
          databaseDir = new File(databaseDir, Long.toString(seed));
        }

        LOG.debug("Setting {} to {}",
                  HiveConf.ConfVars.METASTOREWAREHOUSE.toString(), warehouseDir.getAbsoluteFile());
        System.setProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.toString(), warehouseDir.getAbsolutePath());

        // Set derby log location
        System.setProperty("derby.stream.error.file",
                           cConf.get(Constants.Explore.CFG_LOCAL_DATA_DIR) + File.separator + "derby.log");

        String connectUrl = String.format("jdbc:derby:;databaseName=%s;create=true", databaseDir.getAbsoluteFile());
        LOG.debug("Setting {} to {}", HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(), connectUrl);
        System.setProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(), connectUrl);

        // Some more local mode settings
        System.setProperty(HiveConf.ConfVars.LOCALMODEAUTO.toString(), "true");
        System.setProperty(HiveConf.ConfVars.SUBMITVIACHILD.toString(), "false");
        System.setProperty(MRConfig.FRAMEWORK_NAME, "local");

        // Disable security
        // TODO: verify if auth=NOSASL is really needed - REACTOR-267
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
        setupClasspath();

        // Set local tmp dir to an absolute location in the twill runnable otherwise Hive complains
        System.setProperty(HiveConf.ConfVars.LOCALSCRATCHDIR.toString(),
                           new File(HiveConf.ConfVars.LOCALSCRATCHDIR.defaultVal).getAbsolutePath());
        LOG.info("Setting {} to {}", HiveConf.ConfVars.LOCALSCRATCHDIR.toString(),
                 System.getProperty(HiveConf.ConfVars.LOCALSCRATCHDIR.toString()));


        // We don't support security in Hive Server.
        System.setProperty("hive.server2.authentication", "NONE");
        System.setProperty("hive.server2.enable.doAs", "false");
        System.setProperty("hive.server2.enable.impersonation", "false");

      } catch (Throwable e) {
        throw Throwables.propagate(e);
      }
    }

    @Provides
    @Singleton
    @Exposed
    public final ExploreService providesExploreService(Injector injector, Configuration hConf) {
      // Figure out which HiveExploreService class to load
      Class<? extends ExploreService> hiveExploreServiceCl = ExploreServiceUtils.getHiveService(hConf);
      LOG.info("Using Explore service class {}", hiveExploreServiceCl.getName());
      return injector.getInstance(hiveExploreServiceCl);
    }
  }

  private static void setupClasspath() throws IOException {
    // Here we find the transitive dependencies and remove all paths that come from the boot class path -
    // those paths are not needed because the new JVM will have them in its boot class path.
    // It could even be wrong to keep them because in the target container, the boot class path may be different
    // (for example, if Hadoop uses a different Java version than Reactor).

    Set<String> bootstrapClassPaths = ExploreServiceUtils.getBoostrapClasses();

    Set<File> hBaseTableDeps = ExploreServiceUtils.traceDependencies(
      new HBaseTableUtilFactory().get().getClass().getCanonicalName(),
      bootstrapClassPaths, null);

    // Note the order of dependency jars is important so that HBase jars come first in the classpath order
    // LinkedHashSet maintains insertion order while removing duplicate entries.
    Set<File> orderedDependencies = new LinkedHashSet<File>();
    orderedDependencies.addAll(hBaseTableDeps);
    orderedDependencies.addAll(ExploreServiceUtils.traceDependencies(RemoteDatasetFramework.class.getCanonicalName(),
                                                                     bootstrapClassPaths, null));
    orderedDependencies.addAll(ExploreServiceUtils.traceDependencies(DatasetStorageHandler.class.getCanonicalName(),
                                                                     bootstrapClassPaths, null));

    // Note: the class path entries need to be prefixed with "file://" for the jars to work when
    // Hive starts local map-reduce job.
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (File dep : orderedDependencies) {
      builder.add("file://" + dep.getAbsolutePath());
    }
    List<String> orderedDependenciesStr = builder.build();

    System.setProperty(HiveConf.ConfVars.HIVEAUXJARS.toString(), Joiner.on(',').join(orderedDependenciesStr));
    LOG.debug("Setting {} to {}", HiveConf.ConfVars.HIVEAUXJARS.toString(),
              System.getProperty(HiveConf.ConfVars.HIVEAUXJARS.toString()));

    // Setup HADOOP_CLASSPATH hack, more info on why this is needed - REACTOR-325
    LocalMapreduceClasspathSetter classpathSetter =
      new LocalMapreduceClasspathSetter(new HiveConf(), System.getProperty("java.io.tmpdir"));
    for (File jar : hBaseTableDeps) {
      classpathSetter.accept(jar.getAbsolutePath());
    }
    classpathSetter.setupClasspathScript();
  }
}
