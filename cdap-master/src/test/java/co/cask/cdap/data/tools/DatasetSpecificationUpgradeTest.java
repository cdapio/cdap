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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test {@link co.cask.cdap.data.tools.DatasetSpecificationUpgrader}
 */
public class DatasetSpecificationUpgradeTest {
  private static final String TTL_UPDATED = "table.ttl.migrated.to.seconds";

  @Test
  public void testDatasetSpecificationUpgrade() throws Exception {
    // we can pass null for these parameters as those are not used
    DatasetSpecificationUpgrader datasetSpecificationUpgrader = new DatasetSpecificationUpgrader(null, null);

    DatasetSpecification specification = DatasetSpecification.builder("dataset1", "Table").
      properties(ImmutableMap.of(Table.PROPERTY_TTL, "3600000")).
      datasets(ImmutableList.<DatasetSpecification>of()).build();

    // TTL should be converted to seconds
    DatasetSpecification expected = DatasetSpecification.builder("dataset1", "Table").
      properties(ImmutableMap.of(Table.PROPERTY_TTL, "3600", TTL_UPDATED, "true")).
      datasets(ImmutableList.<DatasetSpecification>of()).build();

    Assert.assertEquals(expected, datasetSpecificationUpgrader.updateTTLInSpecification(specification, null));
    // calling it again to test idempotence
    Assert.assertEquals(expected, datasetSpecificationUpgrader.updateTTLInSpecification(specification, null));

    DatasetSpecification nestedSpec = DatasetSpecification.builder("dataset2", "Table").
      properties(ImmutableMap.of(Table.PROPERTY_TTL, "7200000")).datasets(specification).build();

    // TTL should be converted to seconds for both parent and nested specs
    DatasetSpecification expected2 = DatasetSpecification.builder("dataset2", "Table").
      properties(ImmutableMap.of(Table.PROPERTY_TTL, "7200", TTL_UPDATED, "true")).
      datasets(DatasetSpecification.builder("dataset1", "Table").
        properties(ImmutableMap.of(Table.PROPERTY_TTL, "3600", TTL_UPDATED, "true")).
        datasets(ImmutableList.<DatasetSpecification>of()).
        build()).
      build();
    Assert.assertEquals(expected2, datasetSpecificationUpgrader.updateTTLInSpecification(nestedSpec, null));

    // calling it again to test idempotence
    Assert.assertEquals(expected2, datasetSpecificationUpgrader.updateTTLInSpecification(nestedSpec, null));

    specification = DatasetSpecification.builder("dataset3", "Table").build();
    Assert.assertEquals(specification, datasetSpecificationUpgrader.updateTTLInSpecification(specification, null));

    // calling it again to test idempotence
    Assert.assertEquals(specification, datasetSpecificationUpgrader.updateTTLInSpecification(specification, null));
  }
}
