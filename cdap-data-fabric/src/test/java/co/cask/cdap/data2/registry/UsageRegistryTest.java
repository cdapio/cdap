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

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.metrics.MetricsCollector;
import co.cask.cdap.data2.dataset2.ForwardingDatasetFramework;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

public class UsageRegistryTest extends UsageDatasetTest {

  @Test
  public void testUsageRegistry() {

    // instantiate a usage registry
    UsageRegistry registry = new DefaultUsageRegistry(
      new TransactionExecutorFactory() {
        @Override
        public TransactionExecutor createExecutor(Iterable<TransactionAware> iterable) {
          return dsFrameworkUtil.newInMemoryTransactionExecutor(iterable);
        }
      }, new ForwardingDatasetFramework(dsFrameworkUtil.getFramework()) {
      @Nullable
      @Override
      public <T extends Dataset> T getDataset(DatasetId datasetInstanceId,
                                              Map<String, String> arguments,
                                              @Nullable ClassLoader classLoader)
        throws DatasetManagementException, IOException {

        T t = super.getDataset(datasetInstanceId, arguments, classLoader);
        if (t instanceof UsageDataset) {
          @SuppressWarnings("unchecked")
          T t1 = (T) new WrappedUsageDataset((UsageDataset) t);
          return t1;
        }
        return t;
      }
    });

    // register usage for a stream and a dataset for single and multiple "owners", including a non-program
    registry.register(flow11, datasetInstance1);
    registry.register(flow12, stream1);
    registry.registerAll(ImmutableList.of(flow21, flow22), datasetInstance2);
    registry.registerAll(ImmutableList.of(flow21, flow22), stream1);
    int count = WrappedUsageDataset.registerCount;

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

    // validate that this does re-register previous usages (DefaultUsageRegistry no longer avoids re-registration)
    count += 3;
    Assert.assertEquals(count, WrappedUsageDataset.registerCount);

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

    // validate that this actually re-registered previous usages (through code in wrapped usage dataset)
    Assert.assertEquals(count + 2, WrappedUsageDataset.registerCount);
  }

  /**
   * Usage dataset that delegates all operations to an embedded one,
   * but also counts the number of register() calls in a static variable.
   */
  private static class WrappedUsageDataset extends UsageDataset {

    private static int registerCount = 0;

    final UsageDataset uds;

    public WrappedUsageDataset(UsageDataset uds) {
      super(null);
      this.uds = uds;
    }

    @Override
    public void register(ProgramId programId, DatasetId datasetInstanceId) {
      registerCount++;
      uds.register(programId, datasetInstanceId);
    }

    @Override
    public void register(ProgramId programId, StreamId streamId) {
      registerCount++;
      uds.register(programId, streamId);
    }

    @Override
    public void unregister(ApplicationId applicationId) {
      uds.unregister(applicationId);
    }

    @Override
    public Set<DatasetId> getDatasets(ProgramId programId) {
      return uds.getDatasets(programId);
    }

    @Override
    public Set<DatasetId> getDatasets(ApplicationId applicationId) {
      return uds.getDatasets(applicationId);
    }

    @Override
    public Set<StreamId> getStreams(ProgramId programId) {
      return uds.getStreams(programId);
    }

    @Override
    public Set<StreamId> getStreams(ApplicationId applicationId) {
      return uds.getStreams(applicationId);
    }

    @Override
    public Set<ProgramId> getPrograms(DatasetId datasetInstanceId) {
      return uds.getPrograms(datasetInstanceId);
    }

    @Override
    public Set<ProgramId> getPrograms(StreamId streamId) {
      return uds.getPrograms(streamId);
    }

    @Override
    public void close() throws IOException {
      uds.close();
    }

    @Override
    public void setMetricsCollector(MetricsCollector metricsCollector) {
      uds.setMetricsCollector(metricsCollector);
    }

    @Override
    public void startTx(Transaction tx) {
      uds.startTx(tx);
    }

    @Override
    public void updateTx(Transaction tx) {
      uds.updateTx(tx);
    }

    @Override
    public Collection<byte[]> getTxChanges() {
      return uds.getTxChanges();
    }

    @Override
    public boolean commitTx() throws Exception {
      return uds.commitTx();
    }

    @Override
    public void postTxCommit() {
      uds.postTxCommit();
    }

    @Override
    public boolean rollbackTx() throws Exception {
      return uds.rollbackTx();
    }

    @Override
    public String getTransactionAwareName() {
      return uds.getTransactionAwareName();
    }
  }
}
