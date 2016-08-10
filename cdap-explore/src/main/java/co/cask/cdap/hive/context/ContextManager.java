/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.hive.context;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.ConfigurationUtil;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.security.RemoteUGIProvider;
import co.cask.cdap.data2.security.UGIProvider;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.hive.datasets.DatasetSerDe;
import co.cask.cdap.hive.stream.StreamSerDe;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.RemotePrivilegesManager;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.zookeeper.ZKClientService;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Stores/creates context for Hive queries to run in MapReduce jobs. The Context is used to get dataset and stream
 * information, such as their schema. The context is also used to instantiate datasets.
 *
 * This is a weird class because it is used in two different code paths that call the same method.
 * When Hive executes a query, it calls the SerDe's initialize method. The {@link DatasetSerDe} and
 * {@link StreamSerDe} both use this ContextManager to look up required information. This call to initialize
 * happens both in the process that launches the Hive job (explore service), and in the mapreduce job that was launched.
 *
 * When called from a mapreduce job, we need to create a DatasetFramework, StreamAdmin, and ZKClientService.
 * This is done by deserializing CDAP's configuration from the Hadoop Configuration, creating an injector,
 * and instantiating those object.
 *
 * When called from the explore service, we don't want to instantiate everything all over again for every
 * query, especially since Hive calls initialize multiple times per query for some reason. In that scenario,
 * the explore service calls {@link #saveContext(DatasetFramework, StreamAdmin, SystemDatasetInstantiatorFactory)}
 * when it starts up, in order to cache the Context.
 *
 * Since there is no way for the SerDe to know if it's in a mapreduce job or in the explore service, it relies
 * on whether the Context has been cached to determine whether to create a new Context.
 */
public class ContextManager {
  private static Context savedContext;

  /**
   * Create and save a context, so that any call to {@link #getContext(Configuration)} that is made in this jvm
   * will return the context created from this call.
   */
  public static void saveContext(DatasetFramework datasetFramework, StreamAdmin streamAdmin,
                                 SystemDatasetInstantiatorFactory datasetInstantiatorFactory) {
    savedContext = new Context(datasetFramework, streamAdmin, datasetInstantiatorFactory);
  }

  /**
   * If a context was saved using {@link #saveContext(DatasetFramework, StreamAdmin, SystemDatasetInstantiatorFactory)},
   * returns the saved context. This is what happens in the Explore service.
   * If no context was saved and the conf is not null, creates a context and returns it. The context must be closed by
   * the caller. The context created will not be saved, meaning the next time this method is called,
   * a new context will be created. This is what happens in map reduce jobs launched by Hive.
   * If no context was saved and the conf is null, null is returned.
   *
   * The {@code conf} param is expected to contain serialized {@link co.cask.cdap.common.conf.CConfiguration} and
   * {@link org.apache.hadoop.conf.Configuration} objects, as well as transaction information.
   *
   * @param conf configuration used to create a context, if necessary. If it is null, return the saved context, which
   *             can also be null.
   * @return Context of a query execution.
   * @throws IOException when the configuration does not contain the required settings to create the context
   */
  @Nullable
  public static Context getContext(@Nullable Configuration conf) throws IOException {
    if (conf != null && savedContext == null) {
      return createContext(conf);
    }
    return savedContext;
  }

  @VisibleForTesting
  static Injector createInjector(CConfiguration cConf, Configuration hConf) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new NamespaceClientRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new ExploreClientModule(),
      new ViewAdminModules().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NotificationFeedClientModule(),
      new KafkaClientModule(),
      new AuditModule().getDistributedModules(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new SecureStoreModules().getDistributedModules(),
      new AuthenticationContextModules().getMasterModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(UGIProvider.class).to(RemoteUGIProvider.class).in(Scopes.SINGLETON);
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
          // bind PrivilegesManager to a remote implementation, so it does not need to instantiate the authorizer
          bind(PrivilegesManager.class).to(RemotePrivilegesManager.class);
        }
      }
    );
  }

  // this method is called by the mappers/reducers of jobs launched by Hive.
  private static Context createContext(Configuration conf) throws IOException {
    // Create context needs to happen only when running in as a MapReduce job.
    // In other cases, ContextManager will be initialized using saveContext method.

    CConfiguration cConf = ConfigurationUtil.get(conf, Constants.Explore.CCONF_KEY, CConfCodec.INSTANCE);
    Configuration hConf = ConfigurationUtil.get(conf, Constants.Explore.HCONF_KEY, HConfCodec.INSTANCE);

    Injector injector = createInjector(cConf, hConf);

    ZKClientService zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    StreamAdmin streamAdmin = injector.getInstance(StreamAdmin.class);
    SystemDatasetInstantiatorFactory datasetInstantiatorFactory =
      injector.getInstance(SystemDatasetInstantiatorFactory.class);
    return new Context(datasetFramework, streamAdmin, zkClientService, datasetInstantiatorFactory);
  }

  /**
   * Contains DatasetFramework object and StreamAdmin object required to run Hive queries in MapReduce jobs.
   */
  public static class Context implements Closeable {
    private final DatasetFramework datasetFramework;
    private final StreamAdmin streamAdmin;
    private final ZKClientService zkClientService;
    private final SystemDatasetInstantiatorFactory datasetInstantiatorFactory;

    public Context(DatasetFramework datasetFramework, StreamAdmin streamAdmin,
                   ZKClientService zkClientService,
                   SystemDatasetInstantiatorFactory datasetInstantiatorFactory) {
      // This constructor is called from the MR job Hive launches.
      this.datasetFramework = datasetFramework;
      this.streamAdmin = streamAdmin;
      this.zkClientService = zkClientService;
      this.datasetInstantiatorFactory = datasetInstantiatorFactory;
    }

    public Context(DatasetFramework datasetFramework, StreamAdmin streamAdmin,
                   SystemDatasetInstantiatorFactory datasetInstantiatorFactory) {
      // This constructor is called from Hive server, that is the Explore module.
      this(datasetFramework, streamAdmin, null, datasetInstantiatorFactory);
    }

    public StreamConfig getStreamConfig(Id.Stream streamId) throws IOException {
      return streamAdmin.getConfig(streamId);
    }

    public DatasetSpecification getDatasetSpec(Id.DatasetInstance datasetId) throws DatasetManagementException {
      return datasetFramework.getDatasetSpec(datasetId);
    }

    /**
     * Get a {@link SystemDatasetInstantiator} that can instantiate datasets using the given classloader as the
     * parent classloader for datasets. Must be closed after it is no longer needed, as dataset jars may be unpacked
     * in order to create classloaders for custom datasets.
     *
     * The given parent classloader will be wrapped in a {@link FilterClassLoader}
     * to prevent CDAP dependencies from leaking through. For example, if a custom dataset has an avro dependency,
     * the classloader should use the avro from the custom dataset and not from cdap.
     *
     * @param parentClassLoader the parent classloader to use when instantiating datasets. If null, the system
     *                          classloader will be used
     * @return a dataset instantiator that can be used to instantiate datasets
     */
    public SystemDatasetInstantiator createDatasetInstantiator(@Nullable ClassLoader parentClassLoader) {
      parentClassLoader = parentClassLoader == null ?
        Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), getClass().getClassLoader()) :
        parentClassLoader;
      return datasetInstantiatorFactory.create(FilterClassLoader.create(parentClassLoader));
    }

    @Override
    public void close() {
      // zkClientService is null if used by the Explore service, since Explore manages the lifecycle of the zk service.
      // it is not null if used by a mapreduce job launched by Hive.
      if (zkClientService != null) {
        zkClientService.stopAndWait();
      }
    }
  }
}
