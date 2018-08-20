/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.inmemory;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.DatasetId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link PrefixedNamespaces}.
 */
public class PrefixedNamespacesTest {

  @Test
  public void testNamespacing() {
    CConfiguration cConf = CConfiguration.create();
    DatasetId datasetId = new DatasetId("ns1", "ds1");
    String namespacedName = PrefixedNamespaces.namespace(cConf, datasetId.getNamespace(), datasetId.getDataset());
    Assert.assertEquals("cdap_ns1.ds1", namespacedName);
    Assert.assertEquals(datasetId, PrefixedNamespaces.getDatasetId(cConf, namespacedName));

    cConf.set(Constants.Dataset.TABLE_PREFIX, "tblprefix");
    namespacedName = PrefixedNamespaces.namespace(cConf, datasetId.getNamespace(), datasetId.getDataset());
    Assert.assertEquals("tblprefix_ns1.ds1", namespacedName);
    Assert.assertEquals(datasetId, PrefixedNamespaces.getDatasetId(cConf, namespacedName));
  }

  @Test
  public void testDatasetWithPeriod() {
    CConfiguration cConf = CConfiguration.create();
    DatasetId datasetId = new DatasetId("ns1", "ds.1");
    String namespacedName = PrefixedNamespaces.namespace(cConf, datasetId.getNamespace(), datasetId.getDataset());
    Assert.assertEquals("cdap_ns1.ds.1", namespacedName);
    Assert.assertEquals(datasetId, PrefixedNamespaces.getDatasetId(cConf, namespacedName));

    cConf.set(Constants.Dataset.TABLE_PREFIX, "tblprefix");
    namespacedName = PrefixedNamespaces.namespace(cConf, datasetId.getNamespace(), datasetId.getDataset());
    Assert.assertEquals("tblprefix_ns1.ds.1", namespacedName);
    Assert.assertEquals(datasetId, PrefixedNamespaces.getDatasetId(cConf, namespacedName));
  }
}
