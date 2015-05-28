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

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.proto.Id;
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
 * Stores/creates context for Hive queries to run in MapReduce jobs.
 */
public class ContextManager {
  private static Context savedContext;

  public static void saveContext(DatasetFramework datasetFramework, StreamAdmin streamAdmin,
                                 SystemDatasetInstantiatorFactory datasetInstantiatorFactory) {
    savedContext = new Context(datasetFramework, streamAdmin, datasetInstantiatorFactory);
  }

  /**
   * Get the context of this JVM. If it has already been saved, return it, otherwise create one
   * using the {@code conf} param, which contains serialized {@link co.cask.cdap.common.conf.CConfiguration} and
   * {@link org.apache.hadoop.conf.Configuration} objects, as well as transaction information.
   *
   * @param conf configuration used to create a context, if necessary. If it is null, return the saved context, which
   *             can also be null.
   * @return Context of a query execution.
   * @throws IOException when the configuration does not contain the required settings to create the context
   */
  public static Context getContext(@Nullable Configuration conf) throws IOException {
    if (conf != null && savedContext == null) {
      // Saving the context here is important. This code will be executed in a MR job launched by Hive, and accessed
      // by the DatasetSerDe.initialize method, which needs to access the context when it wants to write to a
      // dataset, so it can cache its name. In that case, conf will be null, and it won't be possible to create a
      // context.
      savedContext = createContext(conf);
    }
    return savedContext;
  }

  private static Context createContext(Configuration conf) throws IOException {
    // Create context needs to happen only when running in as a MapReduce job.
    // In other cases, ContextManager will be initialized using saveContext method.

    CConfiguration cConf = ConfigurationUtil.get(conf, Constants.Explore.CCONF_KEY, CConfCodec.INSTANCE);
    Configuration hConf = ConfigurationUtil.get(conf, Constants.Explore.HCONF_KEY, HConfCodec.INSTANCE);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NotificationFeedClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
        }
      }
    );

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

    public StreamAdmin getStreamAdmin() {
      return streamAdmin;
    }

    public StreamConfig getStreamConfig(Id.Stream streamId) throws IOException {
      return streamAdmin.getConfig(streamId);
    }

    public DatasetSpecification getDatasetSpec(Id.DatasetInstance datasetId) throws DatasetManagementException {
      return datasetFramework.getDatasetSpec(datasetId);
    }

    public SystemDatasetInstantiator createDatasetInstantiator(ClassLoader parentClassLoader) {
      return datasetInstantiatorFactory.create(parentClassLoader);
    }

    @Override
    public void close() {
      if (zkClientService != null) {
        zkClientService.stopAndWait();
      }
    }
  }
}
