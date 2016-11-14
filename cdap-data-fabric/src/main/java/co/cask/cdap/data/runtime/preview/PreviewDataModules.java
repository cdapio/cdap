/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.data.runtime.preview;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.data.runtime.DataFabricLocalModule;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.preview.PreviewDatasetFramework;
import co.cask.cdap.data2.metadata.lineage.LineageStore;
import co.cask.cdap.data2.metadata.lineage.LineageStoreReader;
import co.cask.cdap.data2.metadata.lineage.LineageStoreWriter;
import co.cask.cdap.data2.metadata.store.DefaultMetadataStore;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.writer.BasicLineageWriter;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.registry.DefaultUsageRegistry;
import co.cask.cdap.data2.registry.RuntimeUsageRegistry;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.tephra.TransactionManager;

import java.util.Set;

/**
 * Data fabric modules for preview
 */
public class PreviewDataModules {
  public static final String BASE_DATASET_FRAMEWORK = "basicDatasetFramework";

  public Module getDataFabricModule(final TransactionManager transactionManager) {
    return Modules.override(new DataFabricLocalModule()).with(new AbstractModule() {
      @Override
      protected void configure() {
        // InMemorySystemTxClient uses TransactionManager directly, so we need to share TransactionManager.
        bind(TransactionManager.class).toInstance(transactionManager);
      }
    });
  }

  public Module getDataSetsModule(final DatasetFramework remoteDatasetFramework, final Set<String> datasetNames) {

    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));

        bind(MetadataStore.class).to(DefaultMetadataStore.class);
        expose(MetadataStore.class);

        bind(new TypeLiteral<Set<String>>() { })
          .annotatedWith(Names.named("realDatasets")).toInstance(datasetNames);

        bind(DatasetFramework.class)
          .annotatedWith(Names.named("localDatasetFramework"))
          .to(RemoteDatasetFramework.class);

        bind(DatasetFramework.class).annotatedWith(Names.named("actualDatasetFramework")).
          toInstance(remoteDatasetFramework);

        bind(DatasetFramework.class).
          annotatedWith(Names.named(BASE_DATASET_FRAMEWORK)).
          toProvider(PreviewDatasetFrameworkProvider.class).in(Scopes.SINGLETON);

        bind(DatasetFramework.class).
          toProvider(PreviewDatasetFrameworkProvider.class).in(Scopes.SINGLETON);
        expose(DatasetFramework.class);

        bind(LineageStoreReader.class).to(LineageStore.class);
        bind(LineageStoreWriter.class).to(LineageStore.class);
        // Need to expose LineageStoreReader as it's being used by the LineageHandler (through LineageAdmin)
        expose(LineageStoreReader.class);

        bind(LineageWriter.class).to(BasicLineageWriter.class);
        expose(LineageWriter.class);

        bind(RuntimeUsageRegistry.class).to(DefaultUsageRegistry.class).in(Scopes.SINGLETON);
        expose(RuntimeUsageRegistry.class);

        bind(UsageRegistry.class).to(DefaultUsageRegistry.class).in(Scopes.SINGLETON);
        expose(UsageRegistry.class);
      }
    };
  }

  private static final class PreviewDatasetFrameworkProvider implements Provider<DatasetFramework> {
    private final DatasetFramework inMemoryDatasetFramework;
    private final DatasetFramework remoteDatasetFramework;
    private final Set<String> datasetNames;
    private final AuthenticationContext authenticationContext;
    private final AuthorizationEnforcer authorizationEnforcer;

    @Inject
    PreviewDatasetFrameworkProvider(@Named("localDatasetFramework")DatasetFramework inMemoryDatasetFramework,
                                    @Named("actualDatasetFramework")DatasetFramework remoteDatasetFramework,
                                    @Named("realDatasets") Set<String> datasetNames,
                                    AuthenticationContext authenticationContext,
                                    AuthorizationEnforcer authorizationEnforcer) {
      this.inMemoryDatasetFramework = inMemoryDatasetFramework;
      this.remoteDatasetFramework = remoteDatasetFramework;
      this.datasetNames = datasetNames;
      this.authenticationContext = authenticationContext;
      this.authorizationEnforcer = authorizationEnforcer;
    }

    @Override
    public DatasetFramework get() {
      return new PreviewDatasetFramework(inMemoryDatasetFramework, remoteDatasetFramework, datasetNames,
                                         authenticationContext, authorizationEnforcer);
    }
  }
}
