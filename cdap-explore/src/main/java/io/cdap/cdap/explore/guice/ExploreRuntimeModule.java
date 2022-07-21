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

package io.cdap.cdap.explore.guice;

import com.google.common.base.Throwables;
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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.runtime.RuntimeModule;
import io.cdap.cdap.explore.executor.ExploreExecutorHttpHandler;
import io.cdap.cdap.explore.executor.ExploreExecutorService;
import io.cdap.cdap.explore.executor.ExploreMetadataHttpHandler;
import io.cdap.cdap.explore.executor.ExploreQueryExecutorHttpHandler;
import io.cdap.cdap.explore.executor.ExploreStatusHandler;
import io.cdap.cdap.explore.executor.NamespacedExploreMetadataHttpHandler;
import io.cdap.cdap.explore.executor.NamespacedExploreQueryExecutorHttpHandler;
import io.cdap.cdap.explore.service.ExploreService;
import io.cdap.cdap.explore.service.ExploreServiceUtils;
import io.cdap.cdap.explore.service.hive.Hive14ExploreService;
import io.cdap.cdap.gateway.handlers.CommonHandlers;
import io.cdap.http.HttpHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

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
      Named exploreServiceName = Names.named(Constants.Service.EXPLORE_HTTP_USER_SERVICE);
      Multibinder<HttpHandler> handlerBinder =
          Multibinder.newSetBinder(binder(), HttpHandler.class, exploreServiceName);
      handlerBinder.addBinding().to(NamespacedExploreQueryExecutorHttpHandler.class);
      handlerBinder.addBinding().to(ExploreQueryExecutorHttpHandler.class);
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
        throw Throwables.propagate(ioe);
      }
      return credentialsDir;
    }

    @Singleton
    private static final class ExploreServiceProvider implements Provider<ExploreService> {
      private static final long seed = System.currentTimeMillis();

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

      @Override
      public ExploreService get() {
        File hiveDataDir = new File(cConf.get(Constants.Explore.LOCAL_DATA_DIR));
        File defaultScratchDir = new File(hiveDataDir, cConf.get(Constants.AppFabric.TEMP_DIR));

        // The properties set using setProperty will be included to any new HiveConf object created,
        // at the condition that the configuration is known by Hive, and so is one of the HiveConf.ConfVars
        // variables.

        if (System.getProperty(HiveConf.ConfVars.SCRATCHDIR.toString()) == null) {
          System.setProperty(HiveConf.ConfVars.SCRATCHDIR.toString(), defaultScratchDir.getAbsolutePath());
        }

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
        // In distributed mode, the temp directory is always set to the container local directory
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
    public ExploreService providesExploreService(Injector injector, CConfiguration cConf) {
      // Figure out which HiveExploreService class to load
      Class<? extends ExploreService> hiveExploreServiceCl = ExploreServiceUtils.getHiveService(cConf);
      LOG.info("Using Explore service class {}", hiveExploreServiceCl.getName());
      return injector.getInstance(hiveExploreServiceCl);
    }
  }
}
