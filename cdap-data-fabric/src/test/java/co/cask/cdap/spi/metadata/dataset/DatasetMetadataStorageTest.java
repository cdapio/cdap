/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.spi.metadata.dataset;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocalLocationModule;
import co.cask.cdap.common.guice.NamespaceAdminTestModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.metadata.store.DefaultMetadataStore;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.spi.metadata.Metadata;
import co.cask.cdap.spi.metadata.MetadataMutation.Drop;
import co.cask.cdap.spi.metadata.MetadataMutation.Remove;
import co.cask.cdap.spi.metadata.MetadataMutation.Update;
import co.cask.cdap.spi.metadata.MetadataRecord;
import co.cask.cdap.spi.metadata.MetadataStorage;
import co.cask.cdap.spi.metadata.MetadataStorageTest;
import co.cask.cdap.spi.metadata.ScopedNameOfKind;
import co.cask.cdap.spi.metadata.SearchRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionInMemoryModule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static co.cask.cdap.api.metadata.MetadataScope.SYSTEM;
import static co.cask.cdap.api.metadata.MetadataScope.USER;
import static co.cask.cdap.spi.metadata.MetadataKind.PROPERTY;
import static co.cask.cdap.spi.metadata.MetadataKind.TAG;

public class DatasetMetadataStorageTest extends MetadataStorageTest {

  private static TransactionManager txManager;
  private static DatasetMetadataStorage storage;

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
      new LocalLocationModule(),
      new TransactionInMemoryModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new NamespaceAdminTestModule(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule()
    );
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    storage = injector.getInstance(DatasetMetadataStorage.class);
  }

  @AfterClass
  public static void teardown() {
    txManager.stopAndWait();
  }

  @Override
  protected MetadataStorage getMetadataStorage() {
    return storage;
  }

  // this tests is not in MetadataStorageTest,
  // because it tests result scoring and sorting specific to the dataset-based implementation
  @Test
  public void testSearchWeight() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    String ns = "ns1";
    NamespaceId nsId = new NamespaceId(ns);
    MetadataEntity service1 = nsId.app("app1").service("service1").toMetadataEntity();
    MetadataEntity dataset1 = nsId.dataset("ds1").toMetadataEntity();
    MetadataEntity dataset2 = nsId.dataset("ds2").toMetadataEntity();

    // Add metadata
    String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    Map<String, String> userProps = ImmutableMap.of("key1", "value1", "key2", "value2", "multiword", multiWordValue);
    Map<String, String> systemProps = ImmutableMap.of("sysKey1", "sysValue1");
    Set<String> userTags = ImmutableSet.of("tag1", "tag2");
    Set<String> temporaryUserTags = ImmutableSet.of("tag3", "tag4");
    Map<String, String> dataset1UserProps = ImmutableMap.of("sKey1", "sValuee1 sValuee2");
    Map<String, String> dataset2UserProps = ImmutableMap.of("sKey1", "sValue1 sValue2", "Key1", "Value1");
    Set<String> sysTags = ImmutableSet.of("sysTag1");

    MetadataRecord service1Record = new MetadataRecord(
      service1, union(new Metadata(USER, userTags, userProps), new Metadata(SYSTEM, sysTags, systemProps)));
    mds.apply(new Update(service1Record.getEntity(), service1Record.getMetadata()));

    // dd and then remove some metadata for dataset2
    mds.apply(new Update(dataset2, new Metadata(USER, temporaryUserTags, userProps)));
    mds.apply(new Remove(dataset2, temporaryUserTags.stream()
      .map(tag -> new ScopedNameOfKind(TAG, USER, tag)).collect(Collectors.toSet())));
    mds.apply(new Remove(dataset2, userProps.keySet().stream()
      .map(tag -> new ScopedNameOfKind(PROPERTY, USER, tag)).collect(Collectors.toSet())));

    MetadataRecord dataset1Record = new MetadataRecord(dataset1, new Metadata(USER, tags(), dataset1UserProps));
    MetadataRecord dataset2Record = new MetadataRecord(dataset2, new Metadata(USER, tags(), dataset2UserProps));

    mds.batch(ImmutableList.of(new Update(dataset1Record.getEntity(), dataset1Record.getMetadata()),
                               new Update(dataset2Record.getEntity(), dataset2Record.getMetadata())));

    // Test score and metadata match
    assertInOrder(mds, SearchRequest.of("value1 multiword:av2").addNamespace(ns).build(),
                  service1Record, dataset2Record);
    assertInOrder(mds, SearchRequest.of("value1 sValue*").addNamespace(ns).setLimit(Integer.MAX_VALUE).build(),
                  dataset2Record, dataset1Record, service1Record);
    assertResults(mds, SearchRequest.of("*").addNamespace(ns).setLimit(Integer.MAX_VALUE).build(),
                  dataset2Record, dataset1Record, service1Record);

    // clean up
    mds.batch(ImmutableList.of(new Drop(service1), new Drop(dataset1), new Drop(dataset2)));
  }
}
