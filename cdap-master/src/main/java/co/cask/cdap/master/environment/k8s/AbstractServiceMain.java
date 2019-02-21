/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.master.environment.k8s;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.guice.ConstantTransactionSystemClient;
import co.cask.cdap.common.app.MainClassLoader;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DFSLocationModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.options.Option;
import co.cask.cdap.common.options.OptionsParser;
import co.cask.cdap.common.runtime.DaemonMain;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import co.cask.cdap.data2.transaction.TransactionSystemClientService;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.master.environment.DefaultMasterEnvironmentContext;
import co.cask.cdap.master.environment.MasterEnvironmentExtensionLoader;
import co.cask.cdap.master.spi.environment.MasterEnvironment;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * The abstract base class for writing various CDAP master service main classes.
 */
public abstract class AbstractServiceMain extends DaemonMain {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractServiceMain.class);

  private final List<Service> services = new ArrayList<>();
  private final List<AutoCloseable> closeableResources = new ArrayList<>();
  private Injector injector;
  private MasterEnvironment masterEnv;

  /**
   * Helper method for sub-class to call from static void main.
   *
   * @param mainClass the class of the master main class implementation
   * @param args arguments to main
   * @param <T> type of the master main class
   * @throws Exception if execution failed
   */
  protected static <T extends AbstractServiceMain> void main(Class<T> mainClass, String[] args) throws Exception {
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
  public final void init(String[] args) throws MalformedURLException {
    LOG.info("Initializing master service class {}", getClass().getName());

    ConfigOptions opts = new ConfigOptions();
    OptionsParser.init(opts, args, getClass().getSimpleName(), ProjectInfo.getVersion().toString(), System.out);

    CConfiguration cConf = CConfiguration.create();
    if (opts.extraConfPath != null) {
      cConf.addResource(new File(opts.extraConfPath, "cdap-site.xml").toURI().toURL());
    }

    Configuration hConf = new Configuration();

    MasterEnvironmentExtensionLoader envExtLoader = new MasterEnvironmentExtensionLoader(cConf);
    masterEnv = envExtLoader.get(opts.envProvider);

    if (masterEnv == null) {
      throw new IllegalArgumentException("Unable to find a MasterEnvironment implementation with name "
                                           + opts.envProvider);
    }

    try {
      masterEnv.initialize(new DefaultMasterEnvironmentContext(cConf));
    } catch (Exception e) {
      throw new RuntimeException("Exception raised when initializing master environment for " + masterEnv.getName(), e);
    }

    List<Module> modules = new ArrayList<>();
    modules.add(new ConfigModule(cConf, hConf));
    modules.add(new IOModule());
    modules.add(new DFSLocationModule());
    modules.add(new MetricsClientRuntimeModule().getDistributedModules());
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(DiscoveryService.class)
          .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
        bind(DiscoveryServiceClient.class)
          .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));

        // TODO: Remove when there is a proper LogAppender to use in K8s
        bind(LogAppender.class).to(SysOutLogAppender.class).in(Scopes.SINGLETON);
      }
    });
    modules.addAll(getServiceModules(masterEnv));

    injector = Guice.createInjector(modules);

    // Initialize logging context
    LogAppenderInitializer logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    closeableResources.add(logAppenderInitializer);

    logAppenderInitializer.initialize();
    Optional.ofNullable(getLoggingContext()).ifPresent(LoggingContextAccessor::setLoggingContext);

    // Add Services
    services.add(injector.getInstance(MetricsCollectionService.class));
    addServices(injector, services, closeableResources);

    LOG.info("Master service {} initialized", getClass().getName());
  }

  @Override
  public final void start() {
    LOG.info("Starting all services for master service {}", getClass().getName());
    for (Service service : services) {
      LOG.info("Starting service {} in master service {}", service, getClass().getName());
      service.startAndWait();
    }
    LOG.info("All services for master service {} started", getClass().getName());
  }

  @Override
  public final void stop() {
    // Stop service in reverse order
    LOG.info("Stopping all services for master service {}", getClass().getName());
    for (Service service : Lists.reverse(services)) {
      LOG.info("Stopping service {} in master service {}", service, getClass().getName());
      try {
        service.stopAndWait();
      } catch (Exception e) {
        // Catch and log exception on stopping to make sure each service has a chance to stop
        LOG.warn("Exception raised when stopping service {} in master service {}", service, getClass().getName(), e);
      }
    }

    for (AutoCloseable closeable : closeableResources) {
      try {
        closeable.close();
      } catch (Exception e) {
        // Catch and log exception on stopping to make sure all closeables are closed
        LOG.warn("Exception raised when closing resource {} in master service {}", closeable, getClass().getName(), e);
      }
    }

    LOG.info("All services for master service {} stopped", getClass().getName());
  }

  @Override
  public final void destroy() {
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
    return Modules.override(
      new DataFabricModules("master").getDistributedModules()).with(new AbstractModule() {
      @Override
      protected void configure() {
        // Bind transaction system to a constant one, basically no transaction, with every write become
        // visible immediately.
        // TODO: Ideally we shouldn't need this at all. However, it is needed now to satisfy dependencies
        bind(TransactionSystemClientService.class).to(DelegatingTransactionSystemClientService.class);
        bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
      }
    });
  }

  /**
   * Returns a {@link List} of Guice {@link Module} that this specific for this master service.
   */
  protected abstract List<Module> getServiceModules(MasterEnvironment masterEnv);

  /**
   * Adds {@link Service} to run.
   *
   * @param injector the Guice {@link Injector} for all the necessary bindings
   * @param services the {@link List} to populate services to run
   * @param closeableResources the {@link List} to populate {@link AutoCloseable} that will be closed on stopping
   *                           of this service main
   */
  protected abstract void addServices(Injector injector, List<? super Service> services,
                                      List<? super AutoCloseable> closeableResources);

  /**
   * Returns the {@link LoggingContext} to use for this service main.
   *
   * @return the {@link LoggingContext} or {@code null} to not setting logging context
   */
  @Nullable
  protected abstract LoggingContext getLoggingContext();

  /**
   * Configuration class to help parsing command line arguments
   */
  private static final class ConfigOptions {
    @Option(name = "env", usage = "Name of the CDAP master environment extension provider")
    private String envProvider;

    @Option(name = "conf", usage = "Directory path for CDAP configuration files")
    private String extraConfPath;
  }

  /**
   * The class bridge a {@link Supplier} to Guice {@link Provider}.
   *
   * @param <T> type of the object provided by this {@link Provider}
   */
  protected static final class SupplierProviderBridge<T> implements Provider<T> {

    private final Supplier<T> supplier;

    SupplierProviderBridge(Supplier<T> supplier) {
      this.supplier = supplier;
    }

    @Override
    public T get() {
      return supplier.get();
    }
  }

  /**
   * A {@link LogAppender} that just log with the current log appender.
   * TODO: Remove this class when there is a proper LogAppender for K8s.
   */
  private static final class SysOutLogAppender extends LogAppender {

    @Override
    protected void appendEvent(LogMessage logMessage) {
      ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
      if (loggerFactory instanceof LoggerContext) {
        ch.qos.logback.classic.Logger logger = ((LoggerContext) loggerFactory).getLogger(logMessage.getLoggerName());
        Iterator<Appender<ILoggingEvent>> iterator = logger.iteratorForAppenders();
        while (iterator.hasNext()) {
          Appender<ILoggingEvent> appender = iterator.next();
          if (appender != this) {
            appender.doAppend(logMessage);
          }
        }
      } else {
        System.out.println(logMessage);
      }
    }
  }
}
