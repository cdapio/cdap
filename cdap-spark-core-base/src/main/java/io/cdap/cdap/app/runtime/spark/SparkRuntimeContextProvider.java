/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.spark.dynamic.SparkInterpreter;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.DistributedArtifactManagerModule;
import io.cdap.cdap.app.guice.DistributedProgramContainerModule;
import io.cdap.cdap.app.guice.UnsupportedPluginFinder;
import io.cdap.cdap.app.program.DefaultProgram;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.data.ProgramContextAware;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.BasicProgramContext;
import io.cdap.cdap.internal.app.runtime.ProgramClassLoader;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitors;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.runtime.workflow.NameMappedDatasetFramework;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.spark.SparkConf;
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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
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
  static final String PROGRAM_JAR_EXPANDED_NAME = "program.jar.expanded.zip";
  static final String PROGRAM_JAR_NAME = "program.jar";
  static final String EXECUTOR_CLASSLOADER_NAME = "org.apache.spark.repl.ExecutorClassLoader";

  private static volatile SparkRuntimeContext sparkRuntimeContext;

  /**
   * Returns the current {@link SparkRuntimeContext}.
   */
  public static SparkRuntimeContext get() {
    System.err.println("ashau - getting SparkRuntimeContext");
    if (sparkRuntimeContext != null) {
      System.err.println("ashau - already exists, returning");
      return sparkRuntimeContext;
    }

    // Try to find it from the context classloader
    ClassLoader runtimeClassLoader = Thread.currentThread().getContextClassLoader();

    // For interactive Spark, it uses ExecutorClassLoader, which doesn't follow normal classloader hierarchy.
    // Since the presence of ExecutorClassLoader is optional in Spark, use reflection to gain access to the parentLoader
    if (EXECUTOR_CLASSLOADER_NAME.equals(runtimeClassLoader.getClass().getName())) {
      System.err.println("ashau - in executor classloader");
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
    System.err.println("ashau - sparkClassLoader = " + sparkClassLoader);

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
    System.err.println("ashau - getting spark runtime context");
    if (sparkRuntimeContext != null) {
      System.err.println("ashau - already exists, returning");
      return sparkRuntimeContext;
    }

    try {
      if (!new File(CCONF_FILE_NAME).exists()) {
        System.err.println("ashau - localizing resources");
        localizeResources();
      }
      CConfiguration cConf = createCConf();
      System.err.println("ashau - created cconf");
      Configuration hConf = createHConf();
      System.err.println("ashau - created hconf");

      SparkRuntimeContextConfig contextConfig = new SparkRuntimeContextConfig(hConf);
      ProgramOptions programOptions = contextConfig.getProgramOptions();
      System.err.println("ashau - got program options");

      // Should be yarn only and only for executor node, not the driver node.
      boolean isDriver = contextConfig.isLocal(programOptions);
      System.err.println("ashau - is Driver = " + isDriver);
      /*
      Preconditions.checkState(!isDriver,
                               "SparkContextProvider.getSparkContext should only be called in Spark executor process.");
       */

      ClusterMode clusterMode = ProgramRunners.getClusterMode(programOptions);
      System.err.println("ashau - cluster mode = " + clusterMode);

      // inserted k8s creation
      MasterEnvironment masterEnv = MasterEnvironments.create(cConf, "k8s");
      System.err.println("ashau - masterEnv = " + masterEnv);
      if (masterEnv != null) {
        MasterEnvironmentContext context = MasterEnvironments.createContext(cConf, hConf, masterEnv.getName());
        masterEnv.initialize(context);
        MasterEnvironments.setMasterEnvironment(masterEnv);
        // in k8s, localization of files and archives is handled by the executor itself and not yarn
        // but at this point, the executor has not yet localized hconf, cconf, etc. so we have to do it ourselves
        localizeResources();
      }

      // Create the program
      System.err.println("creating Program");
      Program program = createProgram(cConf, contextConfig);
      System.err.println("created Program");
      ProgramRunId programRunId = program.getId().run(ProgramRunners.getRunId(programOptions));


      System.err.println("creating injector");
      Injector injector = createInjector(cConf, hConf, contextConfig.getProgramId(), programOptions);
      System.err.println("created injector");

      System.err.println("getting log appender");
      LogAppenderInitializer logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
      System.err.println("initializing log appender");
      logAppenderInitializer.initialize();
      System.err.println("initialized log appender");
      SystemArguments.setLogLevel(programOptions.getUserArguments(), logAppenderInitializer);
      ProxySelector oldProxySelector = ProxySelector.getDefault();
      if (clusterMode == ClusterMode.ISOLATED) {
        RuntimeMonitors.setupMonitoring(injector, programOptions);
      }

      MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
      SparkServiceAnnouncer serviceAnnouncer = injector.getInstance(SparkServiceAnnouncer.class);

      Deque<Service> coreServices = new LinkedList<>();

      /*
      if (clusterMode == ClusterMode.ON_PREMISE) {
        // Add ZK for discovery and Kafka
        coreServices.add(injector.getInstance(ZKClientService.class));
        // Add the Kafka client for logs collection
        coreServices.add(injector.getInstance(KafkaClientService.class));
      }
       */

      coreServices.add(metricsCollectionService);
      coreServices.add(serviceAnnouncer);

      System.err.println("starting core services");
      for (Service coreService : coreServices) {
        coreService.startAndWait();
      }

      AtomicBoolean closed = new AtomicBoolean();
      Closeable closeable = () -> {
        if (!closed.compareAndSet(false, true)) {
          return;
        }
        // Close to flush out all important logs
        //logAppenderInitializer.close();

        // Stop all services. Reverse the order.
        for (Service service : (Iterable<Service>) coreServices::descendingIterator) {
          try {
            service.stopAndWait();
          } catch (Exception e) {
            LOG.warn("Exception raised when stopping service {} during program termination.", service, e);
          }
        }
        Authenticator.setDefault(null);
        ProxySelector.setDefault(oldProxySelector);
        LOG.debug("Spark runtime services shutdown completed");
      };

      // Constructor the DatasetFramework
      System.err.println("constructing dataset framework");
      DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
      WorkflowProgramInfo workflowInfo = contextConfig.getWorkflowProgramInfo();
      DatasetFramework programDatasetFramework = workflowInfo == null ?
        datasetFramework :
        NameMappedDatasetFramework.createFromWorkflowProgramInfo(datasetFramework, workflowInfo,
                                                                 contextConfig.getApplicationSpecification());
      // Setup dataset framework context, if required
      if (programDatasetFramework instanceof ProgramContextAware) {
        ((ProgramContextAware) programDatasetFramework).setContext(new BasicProgramContext(programRunId));
      }

      PluginInstantiator pluginInstantiator = createPluginInstantiator(cConf, contextConfig, program.getClassLoader());
      System.err.println("created plugin instantiator");

      // Create the context object
      sparkRuntimeContext = new SparkRuntimeContext(
        contextConfig.getConfiguration(),
        program, programOptions,
        cConf,
        getHostname(),
        injector.getInstance(TransactionSystemClient.class),
        programDatasetFramework,
        metricsCollectionService,
        contextConfig.getWorkflowProgramInfo(),
        pluginInstantiator,
        injector.getInstance(SecureStore.class),
        injector.getInstance(SecureStoreManager.class),
        injector.getInstance(AccessEnforcer.class),
        injector.getInstance(AuthenticationContext.class),
        injector.getInstance(MessagingService.class),
        serviceAnnouncer,
        injector.getInstance(PluginFinder.class),
        injector.getInstance(LocationFactory.class),
        injector.getInstance(MetadataReader.class),
        injector.getInstance(MetadataPublisher.class),
        injector.getInstance(NamespaceQueryAdmin.class),
        injector.getInstance(FieldLineageWriter.class),
        injector.getInstance(RemoteClientFactory.class),
        closeable);
      System.err.println("created runtime context");
      LoggingContextAccessor.setLoggingContext(sparkRuntimeContext.getLoggingContext());
      return sparkRuntimeContext;
    } catch (Exception e) {
      System.err.println("error creating runtime context");
      e.printStackTrace(System.err);
      throw Throwables.propagate(e);
    }
  }

  /*
   */
  private static void localizeResources() throws IOException {
    SparkConf sparkConf = new SparkConf();
    if (sparkConf.get("spark.files", "abc").equals("abc")) {
      System.err.println("spark.files is not set.");
      return;
    }
    String fileStr = sparkConf.get("spark.files");
    List<URI> files = fileStr == null ? Collections.emptyList() :
      Arrays.stream(fileStr.split(",")).map(f -> URI.create(f)).collect(Collectors.toList());
    String archivesStr = sparkConf.get("spark.archives");
    List<URI> archives = archivesStr == null ? Collections.emptyList() :
      Arrays.stream(archivesStr.split(",")).map(f -> URI.create(f)).collect(Collectors.toList());
    org.apache.hadoop.fs.Path targetDir = new org.apache.hadoop.fs.Path("file:" + new File(".").getAbsolutePath());
    Configuration hadoopConf = new Configuration();
    for (URI file : files) {
      FileSystem fs = FileSystem.get(file, hadoopConf);
      org.apache.hadoop.fs.Path sourceFile = new org.apache.hadoop.fs.Path(file);
      org.apache.hadoop.fs.Path destFile = new org.apache.hadoop.fs.Path(targetDir, sourceFile.getName());
      if (!fs.exists(destFile)) {
        System.err.println("Pulling file " + sourceFile.toUri() + " to " + destFile.toUri());
        fs.copyToLocalFile(sourceFile, destFile);
      }
    }
    for (URI archive : archives) {
      FileSystem fs = FileSystem.get(archive, hadoopConf);
      org.apache.hadoop.fs.Path sourceFile = new org.apache.hadoop.fs.Path(archive);
      org.apache.hadoop.fs.Path destFile = new org.apache.hadoop.fs.Path(targetDir, "tmp-" + sourceFile.getName());
      if (fs.exists(destFile)) {
        continue;
      }
      fs.copyToLocalFile(sourceFile, destFile);
      String name = sourceFile.getName().toLowerCase();

      File dstArchive = new File(name);
      if (!dstArchive.exists()) {
        System.err.println("unpacking archive to " + dstArchive.getAbsolutePath());
        if (name.endsWith(".jar")) {
          RunJar.unJar(new File("tmp-" + name), new File(name), RunJar.MATCH_ANY);
        } else if (name.endsWith(".zip")) {
          FileUtil.unZip(new File("tmp-" + name), new File(name));
        }
      }
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
    modules.add(new DistributedProgramContainerModule(cConf, hConf, programId.run(runId), programOptions));

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
   * The {@link ServiceAnnouncer} for announcing user Spark http service. The announcer will be no-op if ZKClient is
   * not present.
   */
  private static final class SparkServiceAnnouncer extends AbstractIdleService implements ServiceAnnouncer {

    private final CConfiguration cConf;
    private final ProgramId programId;
    private ZKClient zkClient;
    private ZKDiscoveryService discoveryService;

    @Inject
    SparkServiceAnnouncer(CConfiguration cConf, ProgramId programId) {
      this.cConf = cConf;
      this.programId = programId;
    }

    @Inject(optional = true)
    void setZKClient(ZKClient zkClient) {
      // Use the ZK path that points to the Twill application of the Spark client.
      String ns = String.format("%s/%s", cConf.get(Constants.CFG_TWILL_ZK_NAMESPACE),
                                ServiceDiscoverable.getName(programId));
      this.zkClient = ZKClients.namespace(zkClient, ns);
    }

    @Override
    public Cancellable announce(String serviceName, int port) {
      return announce(serviceName, port, Bytes.EMPTY_BYTE_ARRAY);
    }

    @Override
    public Cancellable announce(String serviceName, int port, byte[] payload) {
      if (discoveryService == null) {
        return () -> { };
      }
      Discoverable discoverable = new Discoverable(serviceName, new InetSocketAddress(getHostname(), port), payload);
      return discoveryService.register(discoverable);
    }

    @Override
    protected void startUp() throws Exception {
      if (zkClient != null) {
        discoveryService = new ZKDiscoveryService(zkClient);
      }
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
