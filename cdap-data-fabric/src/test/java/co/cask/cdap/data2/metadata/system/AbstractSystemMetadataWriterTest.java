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

package co.cask.cdap.data2.metadata.system;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.metadata.store.DefaultMetadataStore;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.runtime.TransactionInMemoryModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Test AbstractSystemMetadataWriter.
 */
public class AbstractSystemMetadataWriterTest {

  private static TransactionManager txManager;
  private static MetadataStore store;

  @BeforeClass
  public static void setup() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
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
      new NamespaceClientRuntimeModule().getInMemoryModules()
    );
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    store = injector.getInstance(MetadataStore.class);
  }

  @AfterClass
  public static void teardown() {
    txManager.stopAndWait();
  }

  @Test
  public void testMetadataOverwrite() throws Exception {
    Id.DatasetInstance dsInstance = Id.DatasetInstance.from("ns1", "ds1");
    DatasetSystemMetadataWriter datasetSystemMetadataWriter =
      new DatasetSystemMetadataWriter(store, dsInstance,
                                      DatasetProperties.builder()
                                        .add(Table.PROPERTY_TTL, "100")
                                        .build(),
                                      123456L, null, null, "description1");
    datasetSystemMetadataWriter.write();

    MetadataRecord expected =
      new MetadataRecord(dsInstance, MetadataScope.SYSTEM,
                         ImmutableMap.of(AbstractSystemMetadataWriter.DESCRIPTION, "description1",
                                         AbstractSystemMetadataWriter.CREATION_TIME, String.valueOf(123456L),
                                         AbstractSystemMetadataWriter.TTL_KEY, "100"),
                         ImmutableSet.of(dsInstance.getId()));
    Assert.assertEquals(expected, store.getMetadata(MetadataScope.SYSTEM, dsInstance));

    // Now remove TTL, and add dsType
    datasetSystemMetadataWriter =
      new DatasetSystemMetadataWriter(store, dsInstance, DatasetProperties.EMPTY, null, "dsType", "description2");
    datasetSystemMetadataWriter.write();

    expected =
      new MetadataRecord(dsInstance, MetadataScope.SYSTEM,
                         ImmutableMap.of(AbstractSystemMetadataWriter.DESCRIPTION, "description2",
                                         AbstractSystemMetadataWriter.CREATION_TIME, String.valueOf(123456L),
                                         DatasetSystemMetadataWriter.TYPE, "dsType"),
                         ImmutableSet.of(dsInstance.getId()));
    Assert.assertEquals(expected, store.getMetadata(MetadataScope.SYSTEM, dsInstance));

    store.removeMetadata(dsInstance);
  }
}
