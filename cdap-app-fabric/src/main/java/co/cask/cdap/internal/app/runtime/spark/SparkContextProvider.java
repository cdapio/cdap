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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.Services;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Helper class for locating {@link ExecutionSparkContext} from the execution context.
 */
public final class SparkContextProvider {

  // Constants defined for file names used for files localization done by the SparkRuntimeService.
  // They are needed for recreating the ExecutionSparkContext in this class.
  public static final String CCONF_FILE_NAME = "cConf.xml";
  public static final String HCONF_FILE_NAME = "hConf.xml";
  public static final String PROGRAM_JAR_NAME = "program.jar";

  private static volatile ExecutionSparkContext sparkContext;

  /**
   * Returns the current {@link ExecutionSparkContext}.
   */
  public static ExecutionSparkContext getSparkContext() {
    if (sparkContext != null) {
      return sparkContext;
    }

    // Try to find it from the context classloader
    SparkClassLoader sparkClassLoader = ClassLoaders.find(Thread.currentThread().getContextClassLoader(),
                                                          SparkClassLoader.class);
    if (sparkClassLoader != null) {
      // Shouldn't set the sparkContext field. It is because in Standalone, the SparkContext instance will be different
      // for different runs, hence it shouldn't be cached in the static field.
      // In distributed mode, in the driver, the SparkContext will always come from ClassLoader (like in Standalone).
      // Although it can be cached, finding it from ClassLoader with a very minimal performance impact is ok.
      // In the executor process, the static field will be set through the createIfNotExists() call
      // when get called for the first time.
      return sparkClassLoader.getContext();
    }
    return createIfNotExists();
  }

  /**
   * Creates a singleton {@link ExecutionSparkContext}.
   * It has assumption on file location that are localized by the SparkRuntimeService.
   */
  private static synchronized ExecutionSparkContext createIfNotExists() {
    if (sparkContext != null) {
      return sparkContext;
    }

    try {
      CConfiguration cConf = createCConf();
      Configuration hConf = createHConf();

      SparkContextConfig contextConfig = new SparkContextConfig(hConf);

      // Should be yarn only and only for executor node, not the driver node.
      Preconditions.checkState(!contextConfig.isLocal() && Boolean.parseBoolean(System.getenv("SPARK_YARN_MODE")),
                               "SparkContextProvider.getSparkContext should only be called in Spark executor process.");

      // Create the program ClassLoader
      ProgramClassLoader classLoader = ProgramClassLoader.create(new File(PROGRAM_JAR_NAME),
                                                                 SparkClassLoader.class.getClassLoader(),
                                                                 ProgramType.SPARK);
      Injector injector = createInjector(cConf, hConf);

      final Service logAppenderService = new LogAppenderService(injector.getInstance(LogAppenderInitializer.class));
      final ZKClientService zkClientService = injector.getInstance(ZKClientService.class);
      final KafkaClientService kafkaClientService = injector.getInstance(KafkaClientService.class);
      final MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
      final StreamCoordinatorClient streamCoordinatorClient = injector.getInstance(StreamCoordinatorClient.class);

      // Use the shutdown hook to shutdown services, since this class should only be loaded from System classloader
      // of the spark executor, hence there should be exactly one instance only.
      // The problem with not shutting down nicely is that some logs/metrics might be lost
      Services.chainStart(logAppenderService, zkClientService,
                          kafkaClientService, metricsCollectionService, streamCoordinatorClient);
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          // The logger may already been shutdown. Use System.out/err instead
          System.out.println("Shutting SparkClassLoader services");
          Future<List<ListenableFuture<Service.State>>> future = Services.chainStop(logAppenderService,
                                                                                    streamCoordinatorClient,
                                                                                    metricsCollectionService,
                                                                                    kafkaClientService,
                                                                                    zkClientService);
          try {
            List<ListenableFuture<Service.State>> futures = future.get(5, TimeUnit.SECONDS);
            System.out.println("SparkClassLoader services shutdown completed: " + futures);
          } catch (Exception e) {
            System.err.println("Exception when shutting down services");
            e.printStackTrace(System.err);
          }
        }
      });

      // Create the context object
      sparkContext = new ExecutionSparkContext(
        contextConfig.getSpecification(), contextConfig.getProgramId(), contextConfig.getRunId(),
        classLoader, contextConfig.getLogicalStartTime(), contextConfig.getArguments(),
        contextConfig.getTransaction(), injector.getInstance(DatasetFramework.class),
        injector.getInstance(DiscoveryServiceClient.class), metricsCollectionService, hConf,
        injector.getInstance(StreamAdmin.class), contextConfig.getWorkflowToken()
      );
      return sparkContext;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static CConfiguration createCConf() throws MalformedURLException {
    CConfiguration cConf = CConfiguration.create();
    cConf.clear();
    cConf.addResource(new File(CCONF_FILE_NAME).toURI().toURL());
    return cConf;
  }

  private static Configuration createHConf() throws MalformedURLException {
    Configuration hConf = new Configuration();
    hConf.clear();
    hConf.addResource(new File(HCONF_FILE_NAME).toURI().toURL());
    return hConf;
  }

  private static Injector createInjector(CConfiguration cConf, Configuration hConf) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new LoggingModules().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NotificationFeedServiceRuntimeModule().getDistributedModules()
    );
  }

  /**
   * A guava {@link Service} implementation for starting and stopping {@link LogAppenderInitializer}.
   */
  private static final class LogAppenderService extends AbstractService {

    private final LogAppenderInitializer initializer;

    private LogAppenderService(LogAppenderInitializer initializer) {
      this.initializer = initializer;
    }

    @Override
    protected void doStart() {
      try {
        initializer.initialize();
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

  private SparkContextProvider() {
  }
}
