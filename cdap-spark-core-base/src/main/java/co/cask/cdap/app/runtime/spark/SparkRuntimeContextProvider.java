/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.spark.dynamic.SparkInterpreter;
import co.cask.cdap.app.guice.ClusterMode;
import co.cask.cdap.app.guice.DistributedArtifactManagerModule;
import co.cask.cdap.app.guice.DistributedProgramContainerModule;
import co.cask.cdap.app.guice.UnsupportedPluginFinder;
import co.cask.cdap.app.program.DefaultProgram;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.data.ProgramContextAware;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.BasicProgramContext;
import co.cask.cdap.internal.app.runtime.ProgramClassLoader;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.artifact.PluginFinder;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.NameMappedDatasetFramework;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Helper class for locating or creating {@link SparkRuntimeContext} from the execution context.
 */
public final class SparkRuntimeContextProvider {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRuntimeContextProvider.class);

  // Constants defined for file names used for files localization done by the SparkRuntimeService.
  // They are needed for recreating the SparkRuntimeContext in this class.
  static final String CCONF_FILE_NAME = "cConf.xml";
  static final String HCONF_FILE_NAME = "hConf.xml";
  // The suffix has to be .jar, otherwise YARN don't expand it
  static final String PROGRAM_JAR_EXPANDED_NAME = "program.expanded.jar";
  static final String PROGRAM_JAR_NAME = "program.jar";
  static final String EXECUTOR_CLASSLOADER_NAME = "org.apache.spark.repl.ExecutorClassLoader";

  private static volatile SparkRuntimeContext sparkRuntimeContext;

  /**
   * Returns the current {@link SparkRuntimeContext}.
   */
  public static SparkRuntimeContext get() {
    if (sparkRuntimeContext != null) {
      return sparkRuntimeContext;
    }

    // Try to find it from the context classloader
    ClassLoader runtimeClassLoader = Thread.currentThread().getContextClassLoader();

    // For interactive Spark, it uses ExecutorClassLoader, which doesn't follow normal classloader hierarchy.
    // Since the presence of ExecutorClassLoader is optional in Spark, use reflection to gain access to the parentLoader
    if (EXECUTOR_CLASSLOADER_NAME.equals(runtimeClassLoader.getClass().getName())) {
      try {
        Method getParentLoader = runtimeClassLoader.getClass().getDeclaredMethod("parentLoader");
        if (!getParentLoader.isAccessible()) {
          getParentLoader.setAccessible(true);
        }
        runtimeClassLoader = ((ClassLoader) getParentLoader.invoke(runtimeClassLoader)).getParent();
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | ClassCastException e) {
        LOG.warn("Unable to get the CDAP runtime classloader from {}. " +
                   "Spark program may not be running correctly if {} is being used.",
                 EXECUTOR_CLASSLOADER_NAME, SparkInterpreter.class.getName(), e);
      }
    }

    SparkClassLoader sparkClassLoader = ClassLoaders.find(runtimeClassLoader, SparkClassLoader.class);

    if (sparkClassLoader != null) {
      // Shouldn't set the sparkContext field. It is because in Standalone, the SparkContext instance will be different
      // for different runs, hence it shouldn't be cached in the static field.
      // In distributed mode, in the driver, the SparkContext will always come from ClassLoader (like in Standalone).
      // Although it can be cached, finding it from ClassLoader with a very minimal performance impact is ok.
      // In the executor process, the static field will be set through the createIfNotExists() call
      // when get called for the first time.
      return sparkClassLoader.getRuntimeContext();
    }
    return createIfNotExists();
  }

  /**
   * Creates a singleton {@link SparkRuntimeContext}.
   * It has assumption on file location that are localized by the SparkRuntimeService.
   */
  private static synchronized SparkRuntimeContext createIfNotExists() {
    if (sparkRuntimeContext != null) {
      return sparkRuntimeContext;
    }

    try {
      CConfiguration cConf = createCConf();
      Configuration hConf = createHConf();

      SparkRuntimeContextConfig contextConfig = new SparkRuntimeContextConfig(hConf);
      ProgramOptions programOptions = contextConfig.getProgramOptions();

      // Should be yarn only and only for executor node, not the driver node.
      Preconditions.checkState(!contextConfig.isLocal(programOptions)
                                 && Boolean.parseBoolean(System.getenv("SPARK_YARN_MODE")),
                               "SparkContextProvider.getSparkContext should only be called in Spark executor process.");

      // Create the program
      Program program = createProgram(cConf, contextConfig);

      Injector injector = createInjector(cConf, hConf, contextConfig.getProgramId(), programOptions);

      MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
      SparkServiceAnnouncer serviceAnnouncer = injector.getInstance(SparkServiceAnnouncer.class);

      Deque<Service> coreServices = new LinkedList<>();
      coreServices.add(new LogAppenderService(injector.getInstance(LogAppenderInitializer.class), programOptions));
      coreServices.add(injector.getInstance(ZKClientService.class));
      coreServices.add(metricsCollectionService);
      coreServices.add(serviceAnnouncer);

      if (ProgramRunners.getClusterMode(programOptions) == ClusterMode.ON_PREMISE) {
        // Add the Kafka client for logs collection
        coreServices.add(injector.getInstance(KafkaClientService.class));
        // Stream is only supported on premise
        coreServices.add(injector.getInstance(StreamCoordinatorClient.class));
      }

      // Use the shutdown hook to shutdown services, since this class should only be loaded from System classloader
      // of the spark executor, hence there should be exactly one instance only.
      // The problem with not shutting down nicely is that some logs/metrics might be lost
      for (Service coreService : coreServices) {
        coreService.startAndWait();
      }

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          // The logger may already been shutdown. Use System.out/err instead
          System.out.println("Shutting down Spark runtime services");

          // Stop all services. Reverse the order.
          for (Service service : (Iterable<Service>) coreServices::descendingIterator) {
            try {
              service.stopAndWait();
            } catch (Exception e) {
              LOG.warn("Exception raised when stopping service {} during program termination.", service, e);
            }
          }
          System.out.println("Spark runtime services shutdown completed");
        }
      });

      // Constructor the DatasetFramework
      DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
      WorkflowProgramInfo workflowInfo = contextConfig.getWorkflowProgramInfo();
      DatasetFramework programDatasetFramework = workflowInfo == null ?
        datasetFramework :
        NameMappedDatasetFramework.createFromWorkflowProgramInfo(datasetFramework, workflowInfo,
                                                                 contextConfig.getApplicationSpecification());
      // Setup dataset framework context, if required
      if (programDatasetFramework instanceof ProgramContextAware) {
        ProgramRunId programRunId = program.getId().run(ProgramRunners.getRunId(programOptions));
        ((ProgramContextAware) programDatasetFramework).setContext(new BasicProgramContext(programRunId));
      }

      PluginInstantiator pluginInstantiator = createPluginInstantiator(cConf, contextConfig, program.getClassLoader());

      // Create the context object
      sparkRuntimeContext = new SparkRuntimeContext(
        contextConfig.getConfiguration(),
        program, programOptions,
        cConf,
        getHostname(),
        injector.getInstance(TransactionSystemClient.class),
        programDatasetFramework,
        injector.getInstance(DiscoveryServiceClient.class),
        metricsCollectionService,
        injector.getInstance(StreamAdmin.class),
        contextConfig.getWorkflowProgramInfo(),
        pluginInstantiator,
        injector.getInstance(SecureStore.class),
        injector.getInstance(SecureStoreManager.class),
        injector.getInstance(AuthorizationEnforcer.class),
        injector.getInstance(AuthenticationContext.class),
        injector.getInstance(MessagingService.class),
        serviceAnnouncer,
        injector.getInstance(PluginFinder.class),
        injector.getInstance(LocationFactory.class)
      );
      LoggingContextAccessor.setLoggingContext(sparkRuntimeContext.getLoggingContext());
      return sparkRuntimeContext;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static CConfiguration createCConf() throws MalformedURLException {
    return CConfiguration.create(new File(CCONF_FILE_NAME));
  }

  private static Configuration createHConf() throws MalformedURLException {
    Configuration hConf = new Configuration();
    hConf.clear();
    hConf.addResource(new File(HCONF_FILE_NAME).toURI().toURL());
    return hConf;
  }

  private static String getHostname() {
    // Try to determine the hostname from the NM_HOST environment variable, which is set by NM
    String host = System.getenv(ApplicationConstants.Environment.NM_HOST.key());
    if (host != null) {
      return host;
    }
    // If it is missing, use the current hostname
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      // Nothing much we can do. Just throw exception since
      // we need the hostname to start the SparkTransactionService
      throw Throwables.propagate(e);
    }
  }

  private static Program createProgram(CConfiguration cConf,
                                       SparkRuntimeContextConfig contextConfig) throws IOException {
    File programJar = new File(PROGRAM_JAR_NAME);
    File programDir = new File(PROGRAM_JAR_EXPANDED_NAME);

    ClassLoader parentClassLoader = new FilterClassLoader(SparkRuntimeContextProvider.class.getClassLoader(),
                                                          SparkResourceFilters.SPARK_PROGRAM_CLASS_LOADER_FILTER);
    ClassLoader classLoader = new ProgramClassLoader(cConf, programDir, parentClassLoader);

    return new DefaultProgram(new ProgramDescriptor(contextConfig.getProgramId(),
                                                    contextConfig.getApplicationSpecification()),
                              Locations.toLocation(programJar), classLoader);
  }

  @Nullable
  private static PluginInstantiator createPluginInstantiator(CConfiguration cConf,
                                                             SparkRuntimeContextConfig contextConfig,
                                                             ClassLoader parentClassLoader) {
    String pluginArchive = contextConfig.getPluginArchive();
    if (pluginArchive == null) {
      return null;
    }
    return new PluginInstantiator(cConf, parentClassLoader, new File(pluginArchive));
  }

  @VisibleForTesting
  public static Injector createInjector(CConfiguration cConf, Configuration hConf,
                                        ProgramId programId, ProgramOptions programOptions) {
    String runId = programOptions.getArguments().getOption(ProgramOptionConstants.RUN_ID);

    List<Module> modules = new ArrayList<>();
    modules.add(new DistributedProgramContainerModule(cConf, hConf, programId.run(runId),
                                                      programOptions.getArguments()));

    ClusterMode clusterMode = ProgramRunners.getClusterMode(programOptions);
    modules.add(clusterMode == ClusterMode.ON_PREMISE ? new DistributedArtifactManagerModule() : new AbstractModule() {
      @Override
      protected void configure() {
        bind(PluginFinder.class).to(UnsupportedPluginFinder.class);
      }
    });
    return Guice.createInjector(modules);
  }

  /**
   * A guava {@link Service} implementation for starting and stopping {@link LogAppenderInitializer}.
   */
  private static final class LogAppenderService extends AbstractService {

    private final ProgramOptions programOptions;
    private final LogAppenderInitializer initializer;

    private LogAppenderService(LogAppenderInitializer initializer, ProgramOptions programOptions) {
      this.initializer = initializer;
      this.programOptions = programOptions;
    }

    @Override
    protected void doStart() {
      try {
        initializer.initialize();
        SystemArguments.setLogLevel(programOptions.getUserArguments(), initializer);
        notifyStarted();
      } catch (Throwable t) {
        notifyFailed(t);
      }
    }

    @Override
    protected void doStop() {
      try {
        initializer.close();
        notifyStopped();
      } catch (Throwable t) {
        notifyFailed(t);
      }
    }
  }

  /**
   * The {@link ServiceAnnouncer} for announcing user Spark http service.
   */
  private static final class SparkServiceAnnouncer extends AbstractIdleService implements ServiceAnnouncer {

    private final ZKClient zkClient;
    private ZKDiscoveryService discoveryService;

    @Inject
    SparkServiceAnnouncer(CConfiguration cConf, ZKClient zKClient, ProgramId programId) {
      // Use the ZK path that points to the Twill application of the Spark client.
      String ns = String.format("%s/%s", cConf.get(Constants.CFG_TWILL_ZK_NAMESPACE),
                                ServiceDiscoverable.getName(programId));
      this.zkClient = ZKClients.namespace(zKClient, ns);
    }

    @Override
    public Cancellable announce(String serviceName, int port) {
      return announce(serviceName, port, Bytes.EMPTY_BYTE_ARRAY);
    }

    @Override
    public Cancellable announce(String serviceName, int port, byte[] payload) {
      Discoverable discoverable = new Discoverable(serviceName, new InetSocketAddress(getHostname(), port), payload);
      return discoveryService.register(discoverable);
    }

    @Override
    protected void startUp() throws Exception {
      discoveryService = new ZKDiscoveryService(zkClient);
    }

    @Override
    protected void shutDown() throws Exception {
      if (discoveryService != null) {
        discoveryService.close();
      }
    }
  }

  private SparkRuntimeContextProvider() {
    // no-op
  }
}
