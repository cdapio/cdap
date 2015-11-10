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

package co.cask.cdap.data2.registry;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class UsageDatasetTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private final Id.Program flow11 = Id.Program.from("ns1", "app1", ProgramType.FLOW, "flow11");
  private final Id.Program flow12 = Id.Program.from("ns1", "app1", ProgramType.FLOW, "flow12");
  private final Id.Program service11 = Id.Program.from("ns1", "app1", ProgramType.SERVICE, "service11");

  private final Id.Program flow21 = Id.Program.from("ns1", "app2", ProgramType.FLOW, "flow21");
  private final Id.Program flow22 = Id.Program.from("ns1", "app2", ProgramType.FLOW, "flow22");
  private final Id.Program service21 = Id.Program.from("ns1", "app2", ProgramType.SERVICE, "service21");

  private final Id.DatasetInstance datasetInstance1 = Id.DatasetInstance.from("ns1", "ds1");
  private final Id.DatasetInstance datasetInstance2 = Id.DatasetInstance.from("ns1", "ds2");
  private final Id.DatasetInstance datasetInstance3 = Id.DatasetInstance.from("ns1", "ds3");

  private final Id.Stream stream1 = Id.Stream.from("ns1", "s1");
  private final Id.Stream stream2 = Id.Stream.from("ns2", "s1");

  @Test
  public void testOneMapping() throws Exception {
    UsageDataset usageDataset = getUsageDataset("testOneMapping");

    // Add mapping
    usageDataset.register(flow11, datasetInstance1);

    // Verify mapping
    Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageDataset.getDatasets(flow11));
  }

  @Test
  public void testProgramDatasetMapping() throws Exception {
    UsageDataset usageDataset = getUsageDataset("testProgramDatasetMapping");

    // Add mappings
    usageDataset.register(flow11, datasetInstance1);
    usageDataset.register(flow11, datasetInstance3);
    usageDataset.register(flow12, datasetInstance2);
    usageDataset.register(service11, datasetInstance1);

    usageDataset.register(flow21, datasetInstance2);
    usageDataset.register(service21, datasetInstance1);

    // Verify program mappings
    Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance3), usageDataset.getDatasets(flow11));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), usageDataset.getDatasets(flow12));
    Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageDataset.getDatasets(service11));

    Assert.assertEquals(ImmutableSet.of(datasetInstance2), usageDataset.getDatasets(flow21));
    Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageDataset.getDatasets(service21));

    // Verify app mappings
    Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance2, datasetInstance3),
                        usageDataset.getDatasets(flow11.getApplication()));
    Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance2),
                        usageDataset.getDatasets(flow21.getApplication()));

    // Verify dataset mappings
    Assert.assertEquals(ImmutableSet.of(flow11, service11, service21), usageDataset.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.of(flow12, flow21), usageDataset.getPrograms(datasetInstance2));
    Assert.assertEquals(ImmutableSet.of(flow11), usageDataset.getPrograms(datasetInstance3));

    // --------- Delete app1 -----------
    usageDataset.unregister(flow11.getApplication());

    // There should be no mappings for programs of app1 now
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(flow11));
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(flow12));
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(service11));

    Assert.assertEquals(ImmutableSet.of(datasetInstance2), usageDataset.getDatasets(flow21));
    Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageDataset.getDatasets(service21));

    // Verify app mappings
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(flow11.getApplication()));
    Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance2),
                        usageDataset.getDatasets(flow21.getApplication()));

    // Verify dataset mappings
    Assert.assertEquals(ImmutableSet.of(service21), usageDataset.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.of(flow21), usageDataset.getPrograms(datasetInstance2));
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(datasetInstance3));
  }

  @Test
  public void testAllMappings() throws Exception {
    UsageDataset usageDataset = getUsageDataset("testAllMappings");

    // Add mappings
    usageDataset.register(flow11, datasetInstance1);
    usageDataset.register(service21, datasetInstance3);

    usageDataset.register(flow12, stream1);

    // Verify app/adapter mappings
    Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageDataset.getDatasets(flow11));
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(flow12));
    Assert.assertEquals(ImmutableSet.of(datasetInstance3), usageDataset.getDatasets(service21));

    Assert.assertEquals(ImmutableSet.of(stream1), usageDataset.getStreams(flow12));
    Assert.assertEquals(ImmutableSet.<Id.Stream>of(), usageDataset.getStreams(flow22));

    // Verify dataset/stream mappings
    Assert.assertEquals(ImmutableSet.of(flow11), usageDataset.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.of(flow12), usageDataset.getPrograms(stream1));
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(stream2));

    // --------- Delete app1 -----------
    usageDataset.unregister(flow11.getApplication());

    // Verify app/adapter mappings
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(flow11));
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(flow12));
    Assert.assertEquals(ImmutableSet.of(datasetInstance3), usageDataset.getDatasets(service21));

    Assert.assertEquals(ImmutableSet.<Id.Stream>of(), usageDataset.getStreams(flow12));
    Assert.assertEquals(ImmutableSet.<Id.Stream>of(), usageDataset.getStreams(flow22));

    // Verify dataset/stream mappings
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(stream1));
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(stream2));

    // Verify app/adapter mappings
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(flow11));
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(flow12));
    Assert.assertEquals(ImmutableSet.of(datasetInstance3), usageDataset.getDatasets(service21));

    Assert.assertEquals(ImmutableSet.<Id.Stream>of(), usageDataset.getStreams(flow12));
    Assert.assertEquals(ImmutableSet.<Id.Stream>of(), usageDataset.getStreams(flow22));

    // Verify dataset/stream mappings
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(stream1));
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(stream2));

    // Verify app/adapter mappings
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(flow11));
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(flow12));
    Assert.assertEquals(ImmutableSet.of(datasetInstance3), usageDataset.getDatasets(service21));

    Assert.assertEquals(ImmutableSet.<Id.Stream>of(), usageDataset.getStreams(flow12));
    Assert.assertEquals(ImmutableSet.<Id.Stream>of(), usageDataset.getStreams(flow22));

    // Verify dataset/stream mappings
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(stream1));
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(stream2));

    // --------- Delete app2 -----------
    usageDataset.unregister(flow21.getApplication());
    // Verify app/adapter mappings
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(flow11));
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(flow12));
    Assert.assertEquals(ImmutableSet.<Id.DatasetInstance>of(), usageDataset.getDatasets(service21));

    Assert.assertEquals(ImmutableSet.<Id.Stream>of(), usageDataset.getStreams(flow12));
    Assert.assertEquals(ImmutableSet.<Id.Stream>of(), usageDataset.getStreams(flow22));

    // Verify dataset/stream mappings
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(stream1));
    Assert.assertEquals(ImmutableSet.<Id.Program>of(), usageDataset.getPrograms(stream2));
  }

  private static UsageDataset getUsageDataset(String instanceId) throws Exception {
    Id.DatasetInstance id = Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, instanceId);
    return DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), id,
                                           UsageDataset.class.getSimpleName(), DatasetProperties.EMPTY, null, null);
  }
}
