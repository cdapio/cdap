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

package co.cask.cdap.data2.registry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.tephra.TransactionSystemClient;
import org.junit.Assert;
import org.junit.Test;

public class UsageRegistryTest extends UsageDatasetTest {

  @Test
  public void testUsageRegistry() {

    // instantiate a usage registry
    UsageRegistry registry = new BasicUsageRegistry(
      dsFrameworkUtil.getFramework(), dsFrameworkUtil.getInjector().getInstance(TransactionSystemClient.class));

    // register usage for a stream and a store for single and multiple "owners", including a non-program
    registry.register(worker1, datasetInstance1);
    registry.register(worker2, stream1);
    registry.registerAll(ImmutableList.of(worker21, worker22), datasetInstance2);
    registry.registerAll(ImmutableList.of(worker21, worker22), stream1);

    // validate usage
    Assert.assertEquals(ImmutableSet.of(datasetInstance1), registry.getDatasets(worker1));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(worker2));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(worker21));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(worker22));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(worker21));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(worker22));
    Assert.assertEquals(ImmutableSet.of(worker1), registry.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.of(worker21, worker22), registry.getPrograms(datasetInstance2));
    Assert.assertEquals(ImmutableSet.of(worker2, worker21, worker22), registry.getPrograms(stream1));

    // register datasets again
    registry.register(worker1, datasetInstance1);
    registry.registerAll(ImmutableList.of(worker21, worker22), datasetInstance2);

    // validate usage
    Assert.assertEquals(ImmutableSet.of(datasetInstance1), registry.getDatasets(worker1));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(worker2));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(worker21));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(worker22));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(worker21));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(worker22));
    Assert.assertEquals(ImmutableSet.of(worker1), registry.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.of(worker21, worker22), registry.getPrograms(datasetInstance2));
    Assert.assertEquals(ImmutableSet.of(worker2, worker21, worker22), registry.getPrograms(stream1));

    // unregister app
    registry.unregister(worker1.getParent());

    // validate usage for that app is gone
    Assert.assertEquals(ImmutableSet.of(), registry.getDatasets(worker1));
    Assert.assertEquals(ImmutableSet.of(), registry.getStreams(worker2));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(worker21));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(worker22));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(worker21));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(worker22));
    Assert.assertEquals(ImmutableSet.of(), registry.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.of(worker21, worker22), registry.getPrograms(datasetInstance2));
    Assert.assertEquals(ImmutableSet.of(worker21, worker22), registry.getPrograms(stream1));

    // register application 1 again
    registry.register(worker1, datasetInstance1);
    registry.register(worker2, stream1);

    // validate it was re-registered
    Assert.assertEquals(ImmutableSet.of(datasetInstance1), registry.getDatasets(worker1));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(worker2));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(worker21));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(worker22));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(worker21));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(worker22));
    Assert.assertEquals(ImmutableSet.of(worker1), registry.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.of(worker21, worker22), registry.getPrograms(datasetInstance2));
    Assert.assertEquals(ImmutableSet.of(worker2, worker21, worker22), registry.getPrograms(stream1));
  }
}
