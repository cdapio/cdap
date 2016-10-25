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
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.collect.ImmutableSet;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class UsageDatasetTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  protected final NamespaceId namespace1 = new NamespaceId("ns1");
  protected final NamespaceId namespace2 = new NamespaceId("ns2");

  protected final ApplicationId app1 = namespace1.app("app1");
  protected final ProgramId flow11 = app1.flow("flow11");
  protected final ProgramId flow12 = app1.flow("flow12");
  protected final ProgramId service11 = app1.service("service11");

  protected final ApplicationId app2 = namespace1.app("app2");
  protected final ProgramId flow21 = app2.flow("flow21");
  protected final ProgramId flow22 = app2.flow("flow22");
  protected final ProgramId service21 = app2.service("service21");

  protected final DatasetId datasetInstance1 = namespace1.dataset("ds1");
  protected final DatasetId datasetInstance2 = namespace1.dataset("ds2");
  protected final DatasetId datasetInstance3 = namespace1.dataset("ds3");

  protected final StreamId stream1 = namespace1.stream("s1");
  protected final StreamId stream2 = namespace2.stream("s1");

  @Test
  public void testOneMapping() throws Exception {
    final UsageDataset usageDataset = getUsageDataset("testOneMapping");
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) usageDataset);

    // Add mapping
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        usageDataset.register(flow11, datasetInstance1);
      }
    });

    // Verify mapping
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageDataset.getDatasets(flow11));
      }
    });
  }

  @Test
  public void testProgramDatasetMapping() throws Exception {
    final UsageDataset usageDataset = getUsageDataset("testProgramDatasetMapping");
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) usageDataset);

    // Add mappings
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        usageDataset.register(flow11, datasetInstance1);
        usageDataset.register(flow11, datasetInstance3);
        usageDataset.register(flow12, datasetInstance2);
        usageDataset.register(service11, datasetInstance1);

        usageDataset.register(flow21, datasetInstance2);
        usageDataset.register(service21, datasetInstance1);
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Verify program mappings
        Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance3), usageDataset.getDatasets(flow11));
        Assert.assertEquals(ImmutableSet.of(datasetInstance2), usageDataset.getDatasets(flow12));
        Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageDataset.getDatasets(service11));

        Assert.assertEquals(ImmutableSet.of(datasetInstance2), usageDataset.getDatasets(flow21));
        Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageDataset.getDatasets(service21));

        // Verify app mappings
        Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance2, datasetInstance3),
                            usageDataset.getDatasets(flow11.getParent()));
        Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance2),
                            usageDataset.getDatasets(flow21.getParent()));

        // Verify dataset mappings
        Assert.assertEquals(ImmutableSet.of(flow11, service11, service21), usageDataset.getPrograms(datasetInstance1));
        Assert.assertEquals(ImmutableSet.of(flow12, flow21), usageDataset.getPrograms(datasetInstance2));
        Assert.assertEquals(ImmutableSet.of(flow11), usageDataset.getPrograms(datasetInstance3));
      }
    });

    // --------- Delete app1 -----------
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        usageDataset.unregister(flow11.getParent());
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // There should be no mappings for programs of app1 now
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(flow11));
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(flow12));
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(service11));

        Assert.assertEquals(ImmutableSet.of(datasetInstance2), usageDataset.getDatasets(flow21));
        Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageDataset.getDatasets(service21));

        // Verify app mappings
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(flow11.getParent()));
        Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance2),
                            usageDataset.getDatasets(flow21.getParent()));

        // Verify dataset mappings
        Assert.assertEquals(ImmutableSet.of(service21), usageDataset.getPrograms(datasetInstance1));
        Assert.assertEquals(ImmutableSet.of(flow21), usageDataset.getPrograms(datasetInstance2));
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(datasetInstance3));
      }
    });
  }

  @Test
  public void testAllMappings() throws Exception {
    final UsageDataset usageDataset = getUsageDataset("testAllMappings");
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) usageDataset);

    // Add mappings
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        usageDataset.register(flow11, datasetInstance1);
        usageDataset.register(service21, datasetInstance3);

        usageDataset.register(flow12, stream1);
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Verify app mappings
        Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageDataset.getDatasets(flow11));
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(flow12));
        Assert.assertEquals(ImmutableSet.of(datasetInstance3), usageDataset.getDatasets(service21));

        Assert.assertEquals(ImmutableSet.of(stream1), usageDataset.getStreams(flow12));
        Assert.assertEquals(ImmutableSet.<StreamId>of(), usageDataset.getStreams(flow22));

        // Verify dataset/stream mappings
        Assert.assertEquals(ImmutableSet.of(flow11), usageDataset.getPrograms(datasetInstance1));
        Assert.assertEquals(ImmutableSet.of(flow12), usageDataset.getPrograms(stream1));
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(stream2));
      }
    });

    // --------- Delete app1 -----------
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        usageDataset.unregister(flow11.getParent());
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Verify app mappings
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(flow11));
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(flow12));
        Assert.assertEquals(ImmutableSet.of(datasetInstance3), usageDataset.getDatasets(service21));

        Assert.assertEquals(ImmutableSet.<StreamId>of(), usageDataset.getStreams(flow12));
        Assert.assertEquals(ImmutableSet.<StreamId>of(), usageDataset.getStreams(flow22));

        // Verify dataset/stream mappings
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(datasetInstance1));
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(stream1));
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(stream2));

        // Verify app mappings
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(flow11));
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(flow12));
        Assert.assertEquals(ImmutableSet.of(datasetInstance3), usageDataset.getDatasets(service21));

        Assert.assertEquals(ImmutableSet.<StreamId>of(), usageDataset.getStreams(flow12));
        Assert.assertEquals(ImmutableSet.<StreamId>of(), usageDataset.getStreams(flow22));

        // Verify dataset/stream mappings
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(datasetInstance1));
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(stream1));
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(stream2));

        // Verify app mappings
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(flow11));
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(flow12));
        Assert.assertEquals(ImmutableSet.of(datasetInstance3), usageDataset.getDatasets(service21));

        Assert.assertEquals(ImmutableSet.<StreamId>of(), usageDataset.getStreams(flow12));
        Assert.assertEquals(ImmutableSet.<StreamId>of(), usageDataset.getStreams(flow22));

        // Verify dataset/stream mappings
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(datasetInstance1));
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(stream1));
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(stream2));
      }
    });

    // --------- Delete app2 -----------
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        usageDataset.unregister(flow21.getParent());
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Verify app mappings
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(flow11));
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(flow12));
        Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageDataset.getDatasets(service21));

        Assert.assertEquals(ImmutableSet.<StreamId>of(), usageDataset.getStreams(flow12));
        Assert.assertEquals(ImmutableSet.<StreamId>of(), usageDataset.getStreams(flow22));

        // Verify dataset/stream mappings
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(datasetInstance1));
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(stream1));
        Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageDataset.getPrograms(stream2));
      }
    });
  }

  protected static UsageDataset getUsageDataset(String instanceId) throws Exception {
    DatasetId id = DatasetFrameworkTestUtil.NAMESPACE_ID.dataset(instanceId);
    return DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), id,
                                           UsageDataset.class.getSimpleName(), DatasetProperties.EMPTY, null, null);
  }
}
