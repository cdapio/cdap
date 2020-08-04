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
package io.cdap.cdap.data.runtime.preview;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import io.cdap.cdap.data.runtime.DataFabricLevelDBModule;
import io.cdap.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.data2.dataset2.preview.PreviewDatasetFramework;
import io.cdap.cdap.data2.metadata.lineage.DefaultLineageStoreReader;
import io.cdap.cdap.data2.metadata.lineage.LineageStoreReader;
import io.cdap.cdap.data2.metadata.lineage.field.DefaultFieldLineageReader;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageReader;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.LineageWriter;
import io.cdap.cdap.data2.metadata.writer.NoOpLineageWriter;
import io.cdap.cdap.data2.registry.NoOpUsageRegistry;
import io.cdap.cdap.data2.registry.UsageRegistry;
import io.cdap.cdap.data2.registry.UsageWriter;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.noop.NoopMetadataStorage;
import org.apache.tephra.TransactionSystemClient;

/**
 * Data fabric modules for preview
 */
public class PreviewDataModules {
  public static final String BASE_DATASET_FRAMEWORK = "basicDatasetFramework";

  public Module getDataFabricModule(final TransactionSystemClient transactionSystemClient,
                                    final LevelDBTableService levelDBTableService) {
    return Modules.override(new DataFabricLevelDBModule()).with(new AbstractModule() {
      @Override
      protected void configure() {
        // Use the distributed version of the transaction client
        bind(TransactionSystemClient.class).toInstance(transactionSystemClient);
        bind(LevelDBTableService.class).toInstance(levelDBTableService);
      }
    });
  }

  public Module getDataSetsModule(final DatasetFramework remoteDatasetFramework) {

    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(DatasetDefinitionRegistryFactory.class)
          .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);

        bind(MetadataStorage.class).to(NoopMetadataStorage.class);
        expose(MetadataStorage.class);

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

        bind(LineageStoreReader.class).to(DefaultLineageStoreReader.class);
        // Need to expose LineageStoreReader as it's being used by the LineageHandler (through LineageAdmin)
        expose(LineageStoreReader.class);

        bind(FieldLineageReader.class).to(DefaultFieldLineageReader.class);
        expose(FieldLineageReader.class);

        bind(LineageWriter.class).to(NoOpLineageWriter.class);
        expose(LineageWriter.class);

        bind(FieldLineageWriter.class).to(NoOpLineageWriter.class);
        expose(FieldLineageWriter.class);

        bind(UsageWriter.class).to(NoOpUsageRegistry.class).in(Scopes.SINGLETON);
        expose(UsageWriter.class);

        bind(UsageRegistry.class).to(NoOpUsageRegistry.class).in(Scopes.SINGLETON);
        expose(UsageRegistry.class);
      }
    };
  }

  private static final class PreviewDatasetFrameworkProvider implements Provider<DatasetFramework> {
    private final DatasetFramework inMemoryDatasetFramework;
    private final DatasetFramework remoteDatasetFramework;
    private final AuthenticationContext authenticationContext;
    private final AuthorizationEnforcer authorizationEnforcer;

    @Inject
    PreviewDatasetFrameworkProvider(@Named("localDatasetFramework")DatasetFramework inMemoryDatasetFramework,
                                    @Named("actualDatasetFramework")DatasetFramework remoteDatasetFramework,
                                    AuthenticationContext authenticationContext,
                                    AuthorizationEnforcer authorizationEnforcer) {
      this.inMemoryDatasetFramework = inMemoryDatasetFramework;
      this.remoteDatasetFramework = remoteDatasetFramework;
      this.authenticationContext = authenticationContext;
      this.authorizationEnforcer = authorizationEnforcer;
    }

    @Override
    public DatasetFramework get() {
      return new PreviewDatasetFramework(inMemoryDatasetFramework, remoteDatasetFramework,
                                         authenticationContext, authorizationEnforcer);
    }
  }
}
