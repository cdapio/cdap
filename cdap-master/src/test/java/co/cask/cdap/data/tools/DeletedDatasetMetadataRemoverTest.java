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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.InMemoryNamespaceStore;
import co.cask.cdap.data2.metadata.publisher.MetadataChangePublisher;
import co.cask.cdap.data2.metadata.publisher.NoOpMetadataChangePublisher;
import co.cask.cdap.data2.metadata.store.DefaultMetadataStore;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.store.NamespaceStore;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

/**
 * Tests for {@link DeletedDatasetMetadataRemover}.
 */
public class DeletedDatasetMetadataRemoverTest {
  @ClassRule
  public static final DatasetFrameworkTestUtil DS_FRAMEWORK_TEST_UTIL = new DatasetFrameworkTestUtil();

  private static MetadataStore metadataStore;
  private static DeletedDatasetMetadataRemover metadataRemover;

  @BeforeClass
  public static void setup() {
    NamespaceStore nsStore = new InMemoryNamespaceStore();
    nsStore.create(NamespaceMeta.DEFAULT);

    Injector injector = DS_FRAMEWORK_TEST_UTIL.getInjector().createChildInjector(
      new AbstractModule() {
        @Override
        protected void configure() {
          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
          bind(DatasetFramework.class)
            .annotatedWith(Names.named(DataSetsModules.BASE_DATASET_FRAMEWORK))
            .to(InMemoryDatasetFramework.class);
          bind(MetadataChangePublisher.class).to(NoOpMetadataChangePublisher.class);
          bind(MetadataStore.class).to(DefaultMetadataStore.class);
        }
      }
    );
    metadataStore = injector.getInstance(MetadataStore.class);
    metadataRemover = new DeletedDatasetMetadataRemover(nsStore, metadataStore, DS_FRAMEWORK_TEST_UTIL.getFramework());
  }

  @Test
  public void test() throws IOException, DatasetManagementException {
    DatasetId ds1 = NamespaceId.DEFAULT.dataset("ds1");
    DatasetId ds2 = NamespaceId.DEFAULT.dataset("ds2");
    DatasetId ds3 = NamespaceId.DEFAULT.dataset("ds3");
    DS_FRAMEWORK_TEST_UTIL.createInstance("table", ds1.toId(), DatasetProperties.EMPTY);
    DS_FRAMEWORK_TEST_UTIL.createInstance("table", ds2.toId(), DatasetProperties.EMPTY);
    DS_FRAMEWORK_TEST_UTIL.createInstance("table", ds3.toId(), DatasetProperties.EMPTY);
    Assert.assertEquals(3, DS_FRAMEWORK_TEST_UTIL.list(NamespaceId.DEFAULT.toId()).size());
    metadataStore.addTags(MetadataScope.USER, ds1.toId(), "ds1Tag1", "ds1Tag2");
    metadataStore.setProperties(MetadataScope.USER, ds2.toId(), Collections.singletonMap("ds2Key", "ds2Value"));
    metadataStore.addTags(MetadataScope.USER, ds3.toId(), "ds3Tag1", "ds3Tag2");

    verifyMetadataRemoval(ds1);
    verifyMetadataRemoval(ds2);
    verifyMetadataRemoval(ds3);
  }

  private void verifyMetadataRemoval(DatasetId dsId) throws IOException, DatasetManagementException {
    DS_FRAMEWORK_TEST_UTIL.deleteInstance(dsId.toId());
    assertNonEmptyMetadata(dsId);
    metadataRemover.remove();
    assertEmptyMetadata(dsId);
  }

  private void assertNonEmptyMetadata(DatasetId dsId) {
    MetadataRecord metadata = metadataStore.getMetadata(MetadataScope.USER, dsId.toId());
    Assert.assertTrue(!metadata.getProperties().isEmpty() || !metadata.getTags().isEmpty());
  }

  private void assertEmptyMetadata(DatasetId dsId) {
    MetadataRecord metadata = metadataStore.getMetadata(MetadataScope.USER, dsId.toId());
    Assert.assertTrue(metadata.getProperties().isEmpty() && metadata.getTags().isEmpty());
  }
}
