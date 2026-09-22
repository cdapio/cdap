/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.manager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.internal.remote.TaskWorkerManagerService;
import io.cdap.cdap.common.internal.remote.TaskWorkerManagerServiceModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link TwillRunnable} for running {@link TaskWorkerManagerService}.
 */
public class TaskWorkerManagerTwillRunnable extends AbstractTwillRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerManagerTwillRunnable.class);

  private TaskWorkerManagerService taskManagerService;
  private LogAppenderInitializer logAppenderInitializer;

  public TaskWorkerManagerTwillRunnable(String cConfFileName, String hConfFileName) {
    super(ImmutableMap.of("cConf", cConfFileName, "hConf", hConfFileName));
  }

  /**
   * Builds the injector for the proxy container.
   *
   * <p>This is a deliberately small module set. The proxy reads a namespace header, picks a task
   * worker pod, and streams bytes to it; it never publishes metrics, writes audit log entries, or
   * authenticates on its own behalf, so no metrics, audit log, or messaging module is installed.
   * The authentication and remote authenticator modules are present only because
   * {@link RemoteLogAppenderModule} needs them transitively to build its {@code RemoteClient}.
   */
  @VisibleForTesting
  static Injector createInjector(CConfiguration cConf, Configuration hConf) {
    // Unlike the task worker, the proxy has no ZooKeeper/Kafka fallback. It exists to watch
    // Kubernetes V1Endpoints so it can route to individual task worker pods, which has no analogue
    // on the old Hadoop stack, and it is only ever launched from a Kubernetes master environment.
    MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();
    if (masterEnv == null) {
      throw new IllegalStateException(
          "No MasterEnvironment available. The task worker manager proxy is only supported in a "
              + "Kubernetes master environment.");
    }

    List<Module> modules = new ArrayList<>();
    modules.add(new ConfigModule(cConf, hConf));
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(DiscoveryService.class)
            .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
        bind(DiscoveryServiceClient.class)
            .toProvider(
                new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));
      }
    });
    modules.add(RemoteAuthenticatorModules.getDefaultModule());
    modules.add(new AuthenticationContextModules().getMasterWorkerModule());
    modules.add(new RemoteLogAppenderModule());
    modules.add(new TaskWorkerManagerServiceModule());

    return Guice.createInjector(modules);
  }

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);

    try {
      doInitialize();
    } catch (Exception e) {
      LOG.error("Encountered error while initializing TaskWorkerManagerTwillRunnable", e);
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    CompletableFuture<Service.State> future = new CompletableFuture<>();
    taskManagerService.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        future.complete(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        future.completeExceptionally(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    LOG.debug("Starting task worker manager");
    taskManagerService.start();

    try {
      Uninterruptibles.getUninterruptibly(future);
      LOG.debug("Task worker manager stopped");
    } catch (ExecutionException e) {
      LOG.warn("Task worker manager stopped with exception", e);
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping task worker manager");
    if (taskManagerService != null) {
      taskManagerService.stop();
    }
  }

  @Override
  public void destroy() {
    if (logAppenderInitializer != null) {
      logAppenderInitializer.close();
    }
  }

  private void doInitialize() throws Exception {
    CConfiguration cConf = CConfiguration.create(new File(getArgument("cConf")).toURI().toURL());

    Configuration hConf = new Configuration();
    hConf.clear();
    hConf.addResource(new File(getArgument("hConf")).toURI().toURL());

    Injector injector = createInjector(cConf, hConf);

    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    logAppenderInitializer.initialize();

    LoggingContext loggingContext = new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
        Constants.Logging.COMPONENT_NAME,
        TaskWorkerManagerTwillApplication.NAME);
    LoggingContextAccessor.setLoggingContext(loggingContext);

    taskManagerService = injector.getInstance(TaskWorkerManagerService.class);
  }
}
