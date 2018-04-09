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

    // register usage for a stream and a dataset for single and multiple "owners", including a non-program
    registry.register(flow11, datasetInstance1);
    registry.register(flow12, stream1);
    registry.registerAll(ImmutableList.of(flow21, flow22), datasetInstance2);
    registry.registerAll(ImmutableList.of(flow21, flow22), stream1);

    // validate usage
    Assert.assertEquals(ImmutableSet.of(datasetInstance1), registry.getDatasets(flow11));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(flow12));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(flow21));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(flow22));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(flow21));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(flow22));
    Assert.assertEquals(ImmutableSet.of(flow11), registry.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.of(flow21, flow22), registry.getPrograms(datasetInstance2));
    Assert.assertEquals(ImmutableSet.of(flow12, flow21, flow22), registry.getPrograms(stream1));

    // register datasets again
    registry.register(flow11, datasetInstance1);
    registry.registerAll(ImmutableList.of(flow21, flow22), datasetInstance2);

    // validate usage
    Assert.assertEquals(ImmutableSet.of(datasetInstance1), registry.getDatasets(flow11));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(flow12));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(flow21));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(flow22));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(flow21));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(flow22));
    Assert.assertEquals(ImmutableSet.of(flow11), registry.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.of(flow21, flow22), registry.getPrograms(datasetInstance2));
    Assert.assertEquals(ImmutableSet.of(flow12, flow21, flow22), registry.getPrograms(stream1));

    // unregister app
    registry.unregister(flow11.getParent());

    // validate usage for that app is gone
    Assert.assertEquals(ImmutableSet.of(), registry.getDatasets(flow11));
    Assert.assertEquals(ImmutableSet.of(), registry.getStreams(flow12));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(flow21));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(flow22));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(flow21));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(flow22));
    Assert.assertEquals(ImmutableSet.of(), registry.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.of(flow21, flow22), registry.getPrograms(datasetInstance2));
    Assert.assertEquals(ImmutableSet.of(flow21, flow22), registry.getPrograms(stream1));

    // register application 1 again
    registry.register(flow11, datasetInstance1);
    registry.register(flow12, stream1);

    // validate it was re-registered
    Assert.assertEquals(ImmutableSet.of(datasetInstance1), registry.getDatasets(flow11));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(flow12));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(flow21));
    Assert.assertEquals(ImmutableSet.of(datasetInstance2), registry.getDatasets(flow22));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(flow21));
    Assert.assertEquals(ImmutableSet.of(stream1), registry.getStreams(flow22));
    Assert.assertEquals(ImmutableSet.of(flow11), registry.getPrograms(datasetInstance1));
    Assert.assertEquals(ImmutableSet.of(flow21, flow22), registry.getPrograms(datasetInstance2));
    Assert.assertEquals(ImmutableSet.of(flow12, flow21, flow22), registry.getPrograms(stream1));
  }
}
