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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.lib.ObjectMappedTableProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetServiceTestBase;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.TestObject;
import co.cask.cdap.data2.metadata.lineage.LineageDataset;
import co.cask.cdap.data2.registry.UsageDataset;
import co.cask.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class DatasetsUtilTest extends DatasetServiceTestBase {

  @BeforeClass
  public static void setup() throws Exception {
    DatasetServiceTestBase.initialize();
  }

  @Test
  public void testFixProperties() throws DatasetManagementException, UnsupportedTypeException {
    testFix("fileSet",
            FileSetProperties.builder().setBasePath("/tmp/nn").setDataExternal(true).build());
    testFix(FileSet.class.getName(),
            FileSetProperties.builder().setEnableExploreOnCreate(true).setExploreFormat("csv").build());

    testFix("timePartitionedFileSet",
            FileSetProperties.builder().setBasePath("relative").build());
    testFix(TimePartitionedFileSet.class.getName(),
            FileSetProperties.builder().setBasePath("relative").add("custom", "value").build());

    testFix("objectMappedTable",
            ObjectMappedTableProperties.builder().setType(TestObject.class)
              .setRowKeyExploreName("x").setRowKeyExploreType(Schema.Type.STRING)
              .add(Table.PROPERTY_CONFLICT_LEVEL, ConflictDetection.NONE.name()).build());
    testFix(ObjectMappedTable.class.getName(),
            ObjectMappedTableProperties.builder().setType(TestObject.class)
              .setRowKeyExploreName("x").setRowKeyExploreType(Schema.Type.STRING)
              .add(Table.PROPERTY_CONFLICT_LEVEL, ConflictDetection.NONE.name()).build());

    testFix("lineageDataset",
            DatasetProperties.EMPTY);
    testFix(LineageDataset.class.getName(),
            DatasetProperties.builder().add(Table.PROPERTY_TTL, 1000).build());
    testFix(UsageDataset.class.getSimpleName(), DatasetProperties.EMPTY);

    testFix("table",
            DatasetProperties.builder().add(Table.PROPERTY_COLUMN_FAMILY, "fam").build());
    testFix("indexedTable",
            DatasetProperties.builder().add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "a,c").build());
  }

  private void testFix(String type, DatasetProperties props) {
    DatasetDefinition def = DatasetFrameworkTestUtil.getDatasetDefinition(
      inMemoryDatasetFramework, NamespaceId.DEFAULT, type);
    Assert.assertNotNull(def);
    DatasetSpecification spec = def.configure("nn", props);
    Map<String, String> originalProperties = DatasetsUtil.fixOriginalProperties(spec).getOriginalProperties();
    Assert.assertEquals(props.getProperties(), originalProperties);
  }
}
