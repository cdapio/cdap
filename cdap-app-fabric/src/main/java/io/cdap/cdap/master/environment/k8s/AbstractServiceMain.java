/*
 * Copyright © 2019-2022 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.preview.PreviewConfigModule;
import io.cdap.cdap.common.app.MainClassLoader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.common.UncaughtExceptionHandler;
import io.cdap.cdap.common.options.OptionsParser;
import io.cdap.cdap.common.runtime.DaemonMain;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.data.runtime.ConstantTransactionSystemClient;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.guice.CoreSecurityModule;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.spi.data.StorageProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * The abstract base class for writing various service main classes.
 *
 * @param <T> type of options supported by the service.
 */
public abstract class AbstractServiceMain<T extends EnvironmentOptions> extends DaemonMain {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractServiceMain.class);

  private final List<Service> services = new ArrayList<>();
  private final List<AutoCloseable> closeableResources = new ArrayList<>();
  private MasterEnvironment masterEnv;
  private StorageProvider storageProvider;
  private Injector injector;

  /**
   * Helper method for sub-class to call from static void main.
   *
   * @param mainClass the class of the master main class implementation
   * @param args arguments to main
   * @param <T> type of the master main class
   * @throws Exception if execution failed
   */
  protected static <E extends EnvironmentOptions, T extends AbstractServiceMain<E>>
  void main(Class<T> mainClass, String[] args) throws Exception {
    ClassLoader classLoader = MainClassLoader.createFromContext();
    if (classLoader == null) {
      LOG.warn("Failed to create CDAP system ClassLoader. AuthEnforce annotation will not be rewritten.");
      mainClass.newInstance().doMain(args);
    } else {
      Thread.currentThread().setContextClassLoader(classLoader);

      // Use reflection to call doMain in the DaemonMain super class since the ClassLoader is different
      // We need to find the DaemonMain class from the super class chain
      Class<?> cls = classLoader.loadClass(mainClass.getName());
      Class<?> superClass = cls.getSuperclass();
      while (!DaemonMain.class.getName().equals(superClass.getName()) && !Object.class.equals(superClass)) {
        superClass = superClass.getSuperclass();
      }

      if (!DaemonMain.class.getName().equals(superClass.getName())) {
        // This should never happen
        throw new IllegalStateException("Main service class " + mainClass.getName() +
                                          " should inherit from " + DaemonMain.class.getName());
      }

      Method method = superClass.getDeclaredMethod("doMain", String[].class);
      method.setAccessible(true);
      method.invoke(cls.newInstance(), new Object[]{args});
    }
  }

  @Override
  public final void init(String[] args) throws Exception {
    LOG.info("Initializing master service class {}", getClass().getName());

    // System wide setup
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());

    // Intercept JUL loggers
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();

    TypeToken<?> type = TypeToken.of(getClass()).resolveType(AbstractServiceMain.class.getTypeParameters()[0]);
    T options = (T) type.getRawType().newInstance();
    OptionsParser.init(options, args, getClass().getSimpleName(), ProjectInfo.getVersion().toString(), System.out);

    CConfiguration cConf = CConfiguration.create();
    SecurityUtil.loginForMasterService(cConf);

    SConfiguration sConf = SConfiguration.create();
    if (options.getExtraConfPath() != null) {
      cConf.addResource(new File(options.getExtraConfPath(), "cdap-site.xml").toURI().toURL());
      sConf.addResource(new File(options.getExtraConfPath(), "cdap-security.xml").toURI().toURL());
    }
    cConf = updateCConf(cConf);

    Configuration hConf = new Configuration();

    masterEnv = MasterEnvironments.setMasterEnvironment(MasterEnvironments.create(cConf, options.getEnvProvider()));
    MasterEnvironmentContext masterEnvContext = MasterEnvironments.createContext(cConf, hConf, masterEnv.getName());
    masterEnv.initialize(masterEnvContext);

    List<Module> modules = new ArrayList<>();
    modules.add(new ConfigModule(cConf, hConf, sConf));
    modules.add(RemoteAuthenticatorModules.getDefaultModule());
    modules.add(new PreviewConfigModule(cConf, hConf, sConf));
    modules.add(new IOModule());
    modules.add(new MetricsClientRuntimeModule().getDistributedModules());
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(DiscoveryService.class)
          .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
        bind(DiscoveryServiceClient.class)
          .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));
      }
    });
    modules.add(getLogAppenderModule());

    CoreSecurityModule coreSecurityModule = CoreSecurityRuntimeModule.getDistributedModule(cConf);
    modules.add(coreSecurityModule);
    if (coreSecurityModule.requiresZKClient()) {
      modules.add(new ZKClientModule());
    }
    modules.add(new AuthenticationContextModules().getMasterModule());

    modules.addAll(getServiceModules(masterEnv, options, cConf));

    injector = Guice.createInjector(modules);

    // Initialize logging context
    LogAppenderInitializer logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    closeableResources.add(logAppenderInitializer);

    logAppenderInitializer.initialize();
    Optional.ofNullable(getLoggingContext(options)).ifPresent(LoggingContextAccessor::setLoggingContext);

    // Add Services
    services.add(injector.getInstance(MetricsCollectionService.class));
    addServices(injector, services, closeableResources, masterEnv, masterEnvContext, options);

    // Optionally get the storage provider. It is for destroy() method to close it on shutdown.
    Binding<StorageProvider> storageBinding = injector.getExistingBinding(Key.get(StorageProvider.class));
    if (storageBinding != null) {
      storageProvider = storageBinding.getProvider().get();
    }

    LOG.info("Service {} initialized", getClass().getName());
  }

  @Override
  public final void start() {
    LOG.info("Starting all services for {}", getClass().getName());
    for (Service service : services) {
      LOG.info("Starting service {} for {}", service, getClass().getName());
      service.startAndWait();
    }
    LOG.info("All services for {} started", getClass().getName());
  }

  @Override
  public final void stop() {
    LOG.info("Stopping all services for {}", getClass().getName());
    for (Service service : Lists.reverse(services)) {
      LOG.info("Stopping service {} for {}", service, getClass().getName());
      try {
        service.stopAndWait();
      } catch (Exception e) {
        // Catch and log exception on stopping to make sure each service has a chance to stop
        LOG.warn("Exception raised when stopping service {} for {}", service, getClass().getName(), e);
      }
    }

    for (AutoCloseable closeable : closeableResources) {
      try {
        closeable.close();
      } catch (Exception e) {
        // Catch and log exception on stopping to make sure all closeables are closed
        LOG.warn("Exception raised when closing resource {} for {}", closeable, getClass().getName(), e);
      }
    }
    LOG.info("All services for {} stopped", getClass().getName());
  }

  @Override
  public final void destroy() {
    if (storageProvider != null) {
      try {
        storageProvider.close();
      } catch (Exception e) {
        LOG.warn("Exception raised when closing storage provider", e);
      }
    }
    if (masterEnv != null) {
      masterEnv.destroy();
    }
  }

  @VisibleForTesting
  Injector getInjector() {
    return injector;
  }

  /**
   * Returns the Guice module for data-fabric bindings.
   */
  protected final Module getDataFabricModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        install(new StorageModule());
        install(new TransactionExecutorModule());

        // Bind transaction system to a constant one, basically no transaction, with every write become
        // visible immediately.
        // TODO: Ideally we shouldn't need this at all. However, it is needed now to satisfy dependencies
        bind(TransactionSystemClientService.class).to(DelegatingTransactionSystemClientService.class);
        bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
      }
    };
  }

  /**
   * Returns the Guice module for log appender bindings.
   */
  protected Module getLogAppenderModule() {
    return new RemoteLogAppenderModule();
  }

  /**
   * Updates the given {@link CConfiguration}.
   *
   * @param cConf the {@link CConfiguration} to be updated
   * @return the updated configuration
   */
  protected CConfiguration updateCConf(CConfiguration cConf) {
    return cConf;
  }

  /**
   * Returns a {@link List} of Guice {@link Module} that this specific for this master service.
   */
  protected abstract List<Module> getServiceModules(MasterEnvironment masterEnv, T options, CConfiguration cConf);

  /**
   * Adds {@link Service} to run.
   * @param injector the Guice {@link Injector} for all the necessary bindings
   * @param services the {@link List} to populate services to run
   * @param closeableResources the {@link List} to populate {@link AutoCloseable} that will be closed on stopping
   * @param masterEnv the {@link MasterEnvironment} created for this main service
   * @param masterEnvContext the {@link MasterEnvironmentContext} created for this main service
   */
  protected abstract void addServices(Injector injector, List<? super Service> services,
                                      List<? super AutoCloseable> closeableResources,
                                      MasterEnvironment masterEnv, MasterEnvironmentContext masterEnvContext,
                                      T options);

  /**
   * Returns the {@link LoggingContext} to use for this service main.
   *
   * @return the {@link LoggingContext} or {@code null} to not setting logging context
   */
  @Nullable
  protected abstract LoggingContext getLoggingContext(T options);
}
