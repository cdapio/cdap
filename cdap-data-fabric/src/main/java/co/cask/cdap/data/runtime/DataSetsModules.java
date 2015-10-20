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

package co.cask.cdap.data.runtime;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.data2.datafabric.dataset.DatasetProvider;
import co.cask.cdap.data2.datafabric.dataset.DefaultDatasetProvider;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.metadata.publisher.KafkaMetadataChangePublisher;
import co.cask.cdap.data2.metadata.publisher.MetadataChangePublisher;
import co.cask.cdap.data2.metadata.publisher.NoOpMetadataChangePublisher;
import co.cask.cdap.data2.metadata.service.BusinessMetadataStore;
import co.cask.cdap.data2.metadata.service.DefaultBusinessMetadataStore;
import co.cask.cdap.data2.metadata.service.NoOpBusinessMetadataStore;
import co.cask.cdap.data2.metadata.writer.BasicLineageWriter;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

/**
 * DataSets framework bindings
 */
public class DataSetsModules extends RuntimeModule {

  public static final String BASIC_DATASET_FRAMEWORK = "basicDatasetFramework";

  @Override
  public Module getInMemoryModules() {
    return getInMemoryModules(false);
  }

  public Module getInMemoryModules(final boolean publishRequired) {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));

        bind(BusinessMetadataStore.class).to(NoOpBusinessMetadataStore.class);
        expose(BusinessMetadataStore.class);

        bind(DatasetFramework.class)
          .annotatedWith(Names.named(BASIC_DATASET_FRAMEWORK))
          .to(InMemoryDatasetFramework.class).in(Scopes.SINGLETON);
        expose(DatasetFramework.class).annotatedWith(Names.named(BASIC_DATASET_FRAMEWORK));

        bind(DatasetProvider.class)
          .to(DefaultDatasetProvider.class);
        expose(DatasetProvider.class);

        bind(LineageWriter.class).to(BasicLineageWriter.class);
        expose(LineageWriter.class);
        bind(DatasetFramework.class).to(LineageWriterDatasetFramework.class);
        expose(DatasetFramework.class);
        if (publishRequired) {
          bind(MetadataChangePublisher.class).to(KafkaMetadataChangePublisher.class);
        } else {
          bind(MetadataChangePublisher.class).to(NoOpMetadataChangePublisher.class);
        }
        expose(MetadataChangePublisher.class);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));

        bind(BusinessMetadataStore.class).to(DefaultBusinessMetadataStore.class);
        expose(BusinessMetadataStore.class);

        bind(DatasetFramework.class)
          .annotatedWith(Names.named(BASIC_DATASET_FRAMEWORK))
          .to(RemoteDatasetFramework.class);
        expose(DatasetFramework.class).annotatedWith(Names.named(BASIC_DATASET_FRAMEWORK));

        bind(DatasetProvider.class)
          .to(DefaultDatasetProvider.class);
        expose(DatasetProvider.class);

        bind(LineageWriter.class).to(BasicLineageWriter.class);
        expose(LineageWriter.class);
        bind(DatasetFramework.class).to(LineageWriterDatasetFramework.class);
        expose(DatasetFramework.class);
        bind(MetadataChangePublisher.class).to(NoOpMetadataChangePublisher.class);
        expose(MetadataChangePublisher.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return getDistributedModules(false);
  }

  public Module getDistributedModules(final boolean publishRequired) {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));

        bind(BusinessMetadataStore.class).to(DefaultBusinessMetadataStore.class);
        expose(BusinessMetadataStore.class);

        bind(DatasetFramework.class)
          .annotatedWith(Names.named(BASIC_DATASET_FRAMEWORK))
          .to(RemoteDatasetFramework.class);
        expose(DatasetFramework.class).annotatedWith(Names.named(BASIC_DATASET_FRAMEWORK));

        bind(DatasetProvider.class)
          .to(DefaultDatasetProvider.class);
        expose(DatasetProvider.class);

        bind(LineageWriter.class).to(BasicLineageWriter.class);
        expose(LineageWriter.class);
        bind(DatasetFramework.class).to(LineageWriterDatasetFramework.class);
        expose(DatasetFramework.class);
        if (publishRequired) {
          bind(MetadataChangePublisher.class).to(KafkaMetadataChangePublisher.class);
        } else {
          bind(MetadataChangePublisher.class).to(NoOpMetadataChangePublisher.class);
        }
        expose(MetadataChangePublisher.class);
      }
    };
  }
}
