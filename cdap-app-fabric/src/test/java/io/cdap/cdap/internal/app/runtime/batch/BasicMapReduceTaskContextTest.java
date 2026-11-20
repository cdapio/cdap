/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.batch.BatchReadable;
import io.cdap.cdap.api.data.batch.BatchWritable;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.data.batch.SplitReader;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.batch.dataset.CloseableBatchWritable;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link BasicMapReduceTaskContext}.
 */
public class BasicMapReduceTaskContextTest {

  @Test
  public void testBatchReadableGetSplitsPropagatesUncheckedAndWrapsChecked() throws Exception {
    Split expectedSplit = new Split() { };
    FakeBatchDataset dataset = new FakeBatchDataset(Collections.singletonList(expectedSplit));
    BasicMapReduceTaskContext<String, String> context = createTaskContext(dataset);

    BatchReadable<String, String> readable =
        context.getBatchReadable(null, "testDs", Collections.emptyMap());
    List<Split> splits = readable.getSplits();
    Assert.assertEquals(Collections.singletonList(expectedSplit), splits);
    Assert.assertEquals(1, dataset.commitCount);

    IllegalStateException unchecked = new IllegalStateException("flush unchecked failure");
    dataset.commitFailure = unchecked;
    try {
      readable.getSplits();
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(unchecked, thrown);
    }

    Exception checked = new Exception("flush checked failure");
    dataset.commitFailure = checked;
    try {
      readable.getSplits();
      Assert.fail("Expected RuntimeException wrapping checked exception");
    } catch (RuntimeException thrown) {
      Assert.assertSame(checked, thrown.getCause());
    }
    context.close();
  }

  @Test
  public void testBatchReadableSplitReaderClosePropagatesUncheckedAndWrapsChecked() throws Exception {
    Split split = new Split() { };
    FakeBatchDataset dataset = new FakeBatchDataset(Collections.singletonList(split));
    BasicMapReduceTaskContext<String, String> context = createTaskContext(dataset);

    BatchReadable<String, String> readable =
        context.getBatchReadable("default", "testDs", Collections.emptyMap());
    SplitReader<String, String> reader = readable.createSplitReader(split);
    reader.close();
    Assert.assertEquals(1, dataset.readerCloseCount);
    Assert.assertEquals(1, dataset.commitCount);

    IllegalArgumentException unchecked = new IllegalArgumentException("reader close unchecked");
    dataset.commitFailure = unchecked;
    SplitReader<String, String> failingReader = readable.createSplitReader(split);
    try {
      failingReader.close();
      Assert.fail("Expected IllegalArgumentException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(unchecked, thrown);
    }

    Exception checked = new Exception("reader flush checked");
    dataset.commitFailure = checked;
    SplitReader<String, String> checkedReader = readable.createSplitReader(split);
    try {
      checkedReader.close();
      Assert.fail("Expected RuntimeException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(checked, thrown.getCause());
    }
    context.close();
  }

  @Test
  public void testBatchWritableClosePreservesIoExceptionAndWrapsOthers() throws Exception {
    FakeBatchDataset dataset = new FakeBatchDataset(Collections.emptyList());
    BasicMapReduceTaskContext<String, String> context = createTaskContext(dataset);

    CloseableBatchWritable<String, String> writable =
        context.getBatchWritable("default", "testDs", Collections.emptyMap());
    writable.write("k1", "v1");
    Assert.assertEquals("v1", dataset.lastWrittenValue);
    writable.close();
    Assert.assertEquals(1, dataset.commitCount);

    IOException ioFailure = new IOException("direct io failure");
    dataset.commitFailure = ioFailure;
    try {
      writable.close();
      Assert.fail("Expected IOException");
    } catch (IOException thrown) {
      Assert.assertSame(ioFailure, thrown);
    }

    IllegalStateException otherFailure = new IllegalStateException("non-io failure");
    dataset.commitFailure = otherFailure;
    try {
      writable.close();
      Assert.fail("Expected IOException wrapping IllegalStateException");
    } catch (IOException thrown) {
      Assert.assertSame(otherFailure, thrown.getCause());
    }
    context.close();
  }

  private BasicMapReduceTaskContext<String, String> createTaskContext(Dataset dataset)
      throws Exception {
    final CConfiguration cConf = CConfiguration.create();
    ProgramId programId = NamespaceId.DEFAULT.app("mrApp").mr("mrProg");
    MapReduceSpecification mrSpec = new MapReduceSpecification(
        "io.cdap.cdap.DummyMr", "mrProg", "desc", null, null,
        Collections.emptySet(), Collections.emptyMap(), null, null, null, Collections.emptyMap());
    ArtifactId artifactId = new ArtifactId("art", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
        "mrApp", "1.0.0", "desc", null, artifactId,
        Collections.emptyMap(), Collections.emptyMap(),
        ImmutableMap.of("mrProg", mrSpec), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

    Program program = Mockito.mock(Program.class);
    Mockito.when(program.getId()).thenReturn(programId);
    Mockito.when(program.getName()).thenReturn(programId.getProgram());
    Mockito.when(program.getNamespaceId()).thenReturn(programId.getNamespace());
    Mockito.when(program.getApplicationId()).thenReturn(programId.getApplication());
    Mockito.when(program.getType()).thenReturn(ProgramType.MAPREDUCE);
    Mockito.when(program.getApplicationSpecification()).thenReturn(appSpec);
    Mockito.when(program.getClassLoader()).thenReturn(getClass().getClassLoader());

    ProgramOptions options = new SimpleProgramOptions(
        programId,
        new BasicArguments(ImmutableMap.of(
            ProgramOptionConstants.RUN_ID, RunIds.generate().getId(),
            ProgramOptionConstants.ARTIFACT_ID, "default:art:1.0.0")),
        new BasicArguments());

    DatasetFramework dsFramework = Mockito.mock(DatasetFramework.class);
    Mockito.doReturn(dataset).when(dsFramework).getDataset(
        Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

    Transaction tx = new Transaction(10L, 10L, new long[0], new long[0], -1L);
    return new BasicMapReduceTaskContext<>(
        program,
        options,
        cConf,
        null,
        null,
        mrSpec,
        null,
        Mockito.mock(DiscoveryServiceClient.class),
        new NoOpMetricsCollectionService(),
        Mockito.mock(TransactionSystemClient.class),
        tx,
        dsFramework,
        null,
        Collections.emptyMap(),
        Mockito.mock(SecureStore.class),
        Mockito.mock(SecureStoreManager.class),
        Mockito.mock(AccessEnforcer.class),
        Mockito.mock(AuthenticationContext.class),
        Mockito.mock(MessagingService.class),
        Mockito.mock(MapReduceClassLoader.class),
        Mockito.mock(MetadataReader.class),
        Mockito.mock(MetadataPublisher.class),
        Mockito.mock(NamespaceQueryAdmin.class),
        Mockito.mock(FieldLineageWriter.class),
        Mockito.mock(RemoteClientFactory.class),
        Mockito.mock(AppStateStoreProvider.class));
  }

  private static final class FakeBatchDataset
      implements Dataset, BatchReadable<String, String>, BatchWritable<String, String>,
      TransactionAware {

    private final List<Split> splits;
    private Exception commitFailure;
    private int commitCount;
    private int readerCloseCount;
    private String lastWrittenValue;

    private FakeBatchDataset(List<Split> splits) {
      this.splits = splits;
    }

    @Override
    public List<Split> getSplits() {
      return splits;
    }

    @Override
    public SplitReader<String, String> createSplitReader(Split split) {
      return new SplitReader<String, String>() {
        @Override
        public void initialize(Split split) {
          // no-op
        }

        @Override
        public boolean nextKeyValue() {
          return false;
        }

        @Override
        public String getCurrentKey() {
          return null;
        }

        @Override
        public String getCurrentValue() {
          return null;
        }

        @Override
        public void close() {
          readerCloseCount++;
        }
      };
    }

    @Override
    public void write(String key, String value) {
      this.lastWrittenValue = value;
    }

    @Override
    public void startTx(Transaction transaction) {
      // no-op
    }

    @Override
    public void updateTx(Transaction transaction) {
      // no-op
    }

    @Override
    public Collection<byte[]> getTxChanges() {
      return Collections.emptyList();
    }

    @Override
    public boolean commitTx() throws Exception {
      commitCount++;
      if (commitFailure != null) {
        throw commitFailure;
      }
      return true;
    }

    @Override
    public void postTxCommit() {
      // no-op
    }

    @Override
    public boolean rollbackTx() {
      return true;
    }

    @Override
    public String getTransactionAwareName() {
      return "FakeBatchDataset";
    }

    @Override
    public void close() {
      // no-op
    }
  }
}
