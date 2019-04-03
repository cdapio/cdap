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

package io.cdap.cdap.data.runtime;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.runtime.RuntimeModule;
import io.cdap.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.InMemoryDatasetFramework;
import io.cdap.cdap.data2.metadata.AuditMetadataStorage;
import io.cdap.cdap.data2.metadata.lineage.DefaultLineageStoreReader;
import io.cdap.cdap.data2.metadata.lineage.LineageStoreReader;
import io.cdap.cdap.data2.metadata.lineage.field.DefaultFieldLineageReader;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageReader;
import io.cdap.cdap.data2.metadata.writer.BasicLineageWriter;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.LineageWriter;
import io.cdap.cdap.data2.metadata.writer.LineageWriterDatasetFramework;
import io.cdap.cdap.data2.registry.BasicUsageRegistry;
import io.cdap.cdap.data2.registry.UsageRegistry;
import io.cdap.cdap.data2.registry.UsageWriter;
import io.cdap.cdap.metadata.elastic.ElasticsearchMetadataStorage;
import io.cdap.cdap.security.impersonation.OwnerStore;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.dataset.DatasetMetadataStorage;
import io.cdap.cdap.spi.metadata.noop.NoopMetadataStorage;
import io.cdap.cdap.store.DefaultOwnerStore;

/**
 * DataSets framework bindings
 */
public class DataSetsModules extends RuntimeModule {

  public static final String BASE_DATASET_FRAMEWORK = "basicDatasetFramework";
  public static final String SPI_BASE_IMPL = "spiBaseImplementation";

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

        bind(MetadataStorage.class).annotatedWith(Names.named(SPI_BASE_IMPL))
          .toProvider(MetadataStorageProvider.class).in(Scopes.SINGLETON);
        bind(MetadataStorage.class).to(AuditMetadataStorage.class).in(Scopes.SINGLETON);
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
