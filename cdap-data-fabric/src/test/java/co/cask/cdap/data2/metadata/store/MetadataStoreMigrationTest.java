/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.cdap.data2.metadata.store;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.MetadataDatasetDefinition;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionInMemoryModule;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class MetadataStoreMigrationTest {
  private static TransactionManager txManager;
  private static TransactionExecutorFactory txExecutorFactory;
  private static DatasetFramework dsFramework;
  private static MetadataStore store;

  @BeforeClass
  public static void setup() {
    Injector injector = Guice.createInjector(
      new ConfigModule(),
      Modules.override(
        new DataSetsModules().getInMemoryModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          // Need the distributed metadata store.
          bind(MetadataStore.class).to(DefaultMetadataStore.class);
        }
      }),
      new LocationRuntimeModule().getInMemoryModules(),
      new TransactionInMemoryModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new AuditModule().getInMemoryModules()
    );
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    store = injector.getInstance(MetadataStore.class);
    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
    dsFramework = injector.getInstance(DatasetFramework.class);
  }

  @AfterClass
  public static void teardown() {
    txManager.stopAndWait();
  }

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testAddOrUpdateNegativeForUpgrade() throws Exception {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("Metadata migration is in progress. Please retry the same operation " +
                               "once metadata is migrated.");

    // Create v1 table and add values to it.
    DatasetId v1SystemDatasetId = NamespaceId.SYSTEM.dataset("system.metadata");
    DatasetId v1BusinessDatasetId = NamespaceId.SYSTEM.dataset("business.metadata");
    MetadataEntity fieldEntity =
      MetadataEntity.builder(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), "myds"))
        .appendAsType("field", "empname").build();

    ingestData(v1SystemDatasetId, v1BusinessDatasetId, fieldEntity);

    store.setProperties(MetadataScope.SYSTEM, fieldEntity, ImmutableMap.of("newKey", "newValue"));
    store.setProperties(MetadataScope.USER, fieldEntity, ImmutableMap.of("newKey", "newValue"));
  }

  @Test
  public void testAddOrUpdatePositiveForUpgrade() throws Exception {
    // Create v1 table and add values to it.
    DatasetId v1SystemDatasetId = NamespaceId.SYSTEM.dataset("system.metadata");
    DatasetId v1BusinessDatasetId = NamespaceId.SYSTEM.dataset("business.metadata");
    MetadataEntity fieldEntity =
      MetadataEntity.builder(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), "myds"))
        .appendAsType("field", "empname").build();

    MetadataEntity fieldEntity2 =
      MetadataEntity.builder(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), "myds"))
        .appendAsType("field2", "empname").build();

    ingestData(v1SystemDatasetId, v1BusinessDatasetId, fieldEntity);

    store.setProperties(MetadataScope.SYSTEM, fieldEntity2, ImmutableMap.of("newKey", "newValue"));
    store.setProperties(MetadataScope.USER, fieldEntity2, ImmutableMap.of("newKey", "newValue"));
  }

  @Test
  public void testRemoveNegativeForUpgrade() throws Exception {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("Metadata migration is in progress. Please retry the same operation " +
                               "once metadata is migrated.");

    // Create v1 table and add values to it.
    DatasetId v1SystemDatasetId = NamespaceId.SYSTEM.dataset("system.metadata");
    DatasetId v1BusinessDatasetId = NamespaceId.SYSTEM.dataset("business.metadata");
    MetadataEntity fieldEntity =
      MetadataEntity.builder(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), "myds"))
        .appendAsType("field", "empname").build();

    ingestData(v1SystemDatasetId, v1BusinessDatasetId, fieldEntity);

    store.removeProperties(MetadataScope.SYSTEM, fieldEntity);
    store.removeProperties(MetadataScope.USER, fieldEntity);
  }

  @Test
  public void testRemovePositiveForUpgrade() throws Exception {
    // Create v1 table and add values to it.
    DatasetId v1SystemDatasetId = NamespaceId.SYSTEM.dataset("system.metadata");
    DatasetId v1BusinessDatasetId = NamespaceId.SYSTEM.dataset("business.metadata");
    MetadataEntity fieldEntity =
      MetadataEntity.builder(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), "myds"))
        .appendAsType("field", "empname").build();

    MetadataEntity fieldEntity2 =
      MetadataEntity.builder(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), "myds"))
        .appendAsType("field2", "empname").build();

    ingestData(v1SystemDatasetId, v1BusinessDatasetId, fieldEntity);

    store.setProperty(MetadataScope.SYSTEM, fieldEntity2, "key", "value");
    store.setProperty(MetadataScope.USER, fieldEntity2, "key", "value");

    store.removeProperties(MetadataScope.SYSTEM, fieldEntity2);
    store.removeProperties(MetadataScope.USER, fieldEntity2);
  }

  @Test
  public void testGetForUpgrade() throws Exception {
    // Create v1 table and add values to it.
    DatasetId v1SystemDatasetId = NamespaceId.SYSTEM.dataset("system.metadata");
    DatasetId v1BusinessDatasetId = NamespaceId.SYSTEM.dataset("business.metadata");
    MetadataEntity fieldEntity =
      MetadataEntity.builder(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), "myds"))
        .appendAsType("field", "empname").build();
    MetadataEntity fieldEntity2 =
      MetadataEntity.builder(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), "myds"))
        .appendAsType("field2", "empname2").build();

    ingestData(v1SystemDatasetId, v1BusinessDatasetId, fieldEntity);

    Map<String, String> sProps = store.getProperties(MetadataScope.SYSTEM, fieldEntity);
    Assert.assertEquals(0, sProps.size());
    Map<String, String> bProps = store.getProperties(MetadataScope.USER, fieldEntity);
    Assert.assertEquals(0, bProps.size());

    store.setProperty(MetadataScope.SYSTEM, fieldEntity2, "testKey", "testValue");
    store.setProperty(MetadataScope.USER, fieldEntity2, "testKey", "testValue");

    sProps = store.getProperties(MetadataScope.SYSTEM, fieldEntity2);
    Assert.assertEquals(1, sProps.size());

    bProps = store.getProperties(MetadataScope.USER, fieldEntity2);
    Assert.assertEquals(1, bProps.size());

    store.removeProperties(MetadataScope.SYSTEM, fieldEntity2);
    store.removeProperties(MetadataScope.USER, fieldEntity2);

    sProps = store.getProperties(MetadataScope.SYSTEM, fieldEntity2);
    Assert.assertEquals(0, sProps.size());

    bProps = store.getProperties(MetadataScope.USER, fieldEntity2);
    Assert.assertEquals(0, bProps.size());
  }

  /**
   * Gets metadata table.
   */
  private MetadataDataset getMetadataDataset(DatasetId datasetId) throws Exception {
    MetadataScope scope = datasetId.getDataset().contains("business") ? MetadataScope.USER : MetadataScope.SYSTEM;

    if (dsFramework.hasInstance(datasetId)) {
      dsFramework.deleteInstance(datasetId);
    }

    return DatasetsUtil.getOrCreateDataset(dsFramework, datasetId, MetadataDataset.class.getName(),
                                           DatasetProperties.builder()
                                             .add(MetadataDatasetDefinition.SCOPE_KEY, scope.name()).build(),
                                           DatasetDefinition.NO_ARGUMENTS);
  }

  private void execute(TransactionExecutor.Procedure<MetadataDataset> func, DatasetId dsId) throws Exception {
    MetadataDataset metadataDataset = getMetadataDataset(dsId);
    TransactionExecutor txExecutor = Transactions.createTransactionExecutor(txExecutorFactory, metadataDataset);
    txExecutor.executeUnchecked(func, metadataDataset);
  }

  private void ingestData(DatasetId v1S, DatasetId v1B, MetadataEntity fieldEntity) throws Exception {
    execute(mds -> mds.setProperty(fieldEntity, "testKey", "testValue"), v1S);
    execute(mds -> mds.setProperty(fieldEntity, "testKey", "testValue"), v1B);
  }
}
