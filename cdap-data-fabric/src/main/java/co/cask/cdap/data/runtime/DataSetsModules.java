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

package co.cask.cdap.data.runtime;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.metadata.lineage.DefaultLineageStoreReader;
import co.cask.cdap.data2.metadata.lineage.LineageStoreReader;
import co.cask.cdap.data2.metadata.lineage.field.DefaultFieldLineageReader;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageReader;
import co.cask.cdap.data2.metadata.writer.BasicLineageWriter;
import co.cask.cdap.data2.metadata.writer.FieldLineageWriter;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework;
import co.cask.cdap.data2.registry.BasicUsageRegistry;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.registry.UsageWriter;
import co.cask.cdap.metadata.elastic.ElasticsearchMetadataStorage;
import co.cask.cdap.security.impersonation.OwnerStore;
import co.cask.cdap.spi.metadata.MetadataStorage;
import co.cask.cdap.spi.metadata.dataset.DatasetMetadataStorage;
import co.cask.cdap.spi.metadata.noop.NoopMetadataStorage;
import co.cask.cdap.store.DefaultOwnerStore;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

/**
 * DataSets framework bindings
 */
public class DataSetsModules extends RuntimeModule {

  public static final String BASE_DATASET_FRAMEWORK = "basicDatasetFramework";

  @Override
  public Module getInMemoryModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(DatasetDefinitionRegistryFactory.class)
          .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);

        bind(MetadataStorage.class).to(NoopMetadataStorage.class);
        expose(MetadataStorage.class);

        bind(DatasetFramework.class)
          .annotatedWith(Names.named(BASE_DATASET_FRAMEWORK))
          .to(InMemoryDatasetFramework.class).in(Scopes.SINGLETON);

        bind(LineageStoreReader.class).to(DefaultLineageStoreReader.class);
        // Need to expose LineageStoreReader as it's being used by the LineageHandler (through LineageAdmin)
        expose(LineageStoreReader.class);

        bind(FieldLineageReader.class).to(DefaultFieldLineageReader.class);
        expose(FieldLineageReader.class);

        bind(LineageWriter.class).to(BasicLineageWriter.class);
        expose(LineageWriter.class);

        bind(FieldLineageWriter.class).to(BasicLineageWriter.class);
        expose(FieldLineageWriter.class);

        bind(UsageRegistry.class).to(BasicUsageRegistry.class).in(Scopes.SINGLETON);
        expose(UsageRegistry.class);
        bind(UsageWriter.class).to(BasicUsageRegistry.class).in(Scopes.SINGLETON);
        expose(UsageWriter.class);
        bind(BasicUsageRegistry.class).in(Scopes.SINGLETON);

        bind(DatasetFramework.class).to(LineageWriterDatasetFramework.class);
        expose(DatasetFramework.class);

        bind(DefaultOwnerStore.class).in(Scopes.SINGLETON);
        bind(OwnerStore.class).to(DefaultOwnerStore.class);
        expose(OwnerStore.class);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return getModule();
  }

  @Override
  public Module getDistributedModules() {
    return getModule();
  }

  private Module getModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(DatasetDefinitionRegistryFactory.class)
          .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);

        bind(MetadataStorage.class).toProvider(MetadataStorageProvider.class);
        expose(MetadataStorage.class);

        bind(DatasetFramework.class)
          .annotatedWith(Names.named(BASE_DATASET_FRAMEWORK))
          .to(RemoteDatasetFramework.class);

        bind(LineageStoreReader.class).to(DefaultLineageStoreReader.class);
        // Need to expose LineageStoreReader as it's being used by the LineageHandler (through LineageAdmin)
        expose(LineageStoreReader.class);

        bind(FieldLineageReader.class).to(DefaultFieldLineageReader.class);
        expose(FieldLineageReader.class);

        bind(LineageWriter.class).to(BasicLineageWriter.class);
        expose(LineageWriter.class);

        bind(FieldLineageWriter.class).to(BasicLineageWriter.class);
        expose(FieldLineageWriter.class);

        bind(UsageRegistry.class).to(BasicUsageRegistry.class).in(Scopes.SINGLETON);
        expose(UsageRegistry.class);
        bind(UsageWriter.class).to(BasicUsageRegistry.class).in(Scopes.SINGLETON);
        expose(UsageWriter.class);
        bind(BasicUsageRegistry.class).in(Scopes.SINGLETON);

        bind(DatasetFramework.class).to(LineageWriterDatasetFramework.class);
        expose(DatasetFramework.class);

        bind(DefaultOwnerStore.class).in(Scopes.SINGLETON);
        bind(OwnerStore.class).to(DefaultOwnerStore.class);
        expose(OwnerStore.class);
      }
    };
  }
}

// TODO (CDAP-14918): Move binding for MetadataStorage out of DatasetsModules,
// TODO (CDAP-14918): so that data-fabric does not need to depend on metadata.
class MetadataStorageProvider implements Provider<MetadataStorage> {

  private final Injector injector;
  private final CConfiguration cConf;

  @Inject
  MetadataStorageProvider(CConfiguration cConf, Injector injector) {
    this.cConf = cConf;
    this.injector = injector;
  }

  @Override
  public MetadataStorage get() {
    String config = cConf.get(Constants.Metadata.STORAGE_PROVIDER_IMPLEMENTATION,
                              Constants.Metadata.STORAGE_PROVIDER_NOSQL);
    if (Constants.Metadata.STORAGE_PROVIDER_NOSQL.equalsIgnoreCase(config)) {
      return injector.getInstance(DatasetMetadataStorage.class);
    }
    if (Constants.Metadata.STORAGE_PROVIDER_ELASTICSEARCH.equalsIgnoreCase(config)) {
      return injector.getInstance(ElasticsearchMetadataStorage.class);
    }
    throw new IllegalArgumentException("Unsupported MetadataStorage '" + config + "'. Only '" +
                                         Constants.Metadata.STORAGE_PROVIDER_NOSQL + "' and '" +
                                         Constants.Metadata.STORAGE_PROVIDER_ELASTICSEARCH + "' are allowed.");
  }
}
