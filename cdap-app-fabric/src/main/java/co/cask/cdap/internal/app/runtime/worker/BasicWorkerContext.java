/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.worker;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.stream.StreamBatchWriter;
import co.cask.cdap.api.data.stream.StreamWriter;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.stream.StreamWriterFactory;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.logging.context.WorkerLoggingContext;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link WorkerContext}
 */
final class BasicWorkerContext extends AbstractContext implements WorkerContext {
  private static final Logger LOG = LoggerFactory.getLogger(BasicWorkerContext.class);

  private final WorkerSpecification specification;
  private final int instanceId;
  private final LoggingContext loggingContext;
  private volatile int instanceCount;
  private final StreamWriter streamWriter;

  BasicWorkerContext(WorkerSpecification spec, Program program, ProgramOptions programOptions,
                     int instanceId, int instanceCount,
                     MetricsCollectionService metricsCollectionService,
                     DatasetFramework datasetFramework,
                     TransactionSystemClient transactionSystemClient,
                     DiscoveryServiceClient discoveryServiceClient,
                     StreamWriterFactory streamWriterFactory,
                     @Nullable PluginInstantiator pluginInstantiator,
                     SecureStore secureStore,
                     SecureStoreManager secureStoreManager) {
    super(program, programOptions, spec.getDatasets(),
          datasetFramework, transactionSystemClient, discoveryServiceClient, true,
          metricsCollectionService, ImmutableMap.of(Constants.Metrics.Tag.INSTANCE_ID, String.valueOf(instanceId)),
          secureStore, secureStoreManager, pluginInstantiator);

    this.specification = spec;
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.loggingContext = createLoggingContext(program.getId(), getRunId());
    this.streamWriter = streamWriterFactory.create(new Id.Run(program.getId(), getRunId().getId()), getOwners());
  }

  private LoggingContext createLoggingContext(Id.Program programId, RunId runId) {
    return new WorkerLoggingContext(programId.getNamespaceId(), programId.getApplicationId(), programId.getId(),
                                    runId.getId(), String.valueOf(getInstanceId()));
  }

  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  @Override
  public WorkerSpecification getSpecification() {
    return specification;
  }

  @Override
  public void execute(TxRunnable runnable) {
    final TransactionContext context = datasetCache.newTransactionContext();
    try {
      context.start();
      runnable.run(datasetCache);
      context.finish();
    } catch (TransactionFailureException e) {
      abortTransaction(e, "Failed to commit. Aborting transaction.", context);
    } catch (Exception e) {
      abortTransaction(e, "Exception occurred running user code. Aborting transaction.", context);
    }
  }

  @Override
  public int getInstanceCount() {
    return instanceCount;
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }

  public void setInstanceCount(int instanceCount) {
    this.instanceCount = instanceCount;
  }

  private void abortTransaction(Exception e, String message, TransactionContext context) {
    try {
      LOG.error(message, e);
      context.abort();
      throw Throwables.propagate(e);
    } catch (TransactionFailureException e1) {
      LOG.error("Failed to abort transaction.", e1);
      throw Throwables.propagate(e1);
    }
  }

  @Override
  public void write(String stream, String data) throws IOException {
    streamWriter.write(stream, data);
  }

  @Override
  public void write(String stream, String data, Map<String, String> headers) throws IOException {
    streamWriter.write(stream, data, headers);
  }

  @Override
  public void write(String stream, ByteBuffer data) throws IOException {
    streamWriter.write(stream, data);
  }

  @Override
  public void write(String stream, StreamEventData data) throws IOException {
    streamWriter.write(stream, data);
  }

  @Override
  public void writeFile(String stream, File file, String contentType) throws IOException {
    streamWriter.writeFile(stream, file, contentType);
  }

  @Override
  public StreamBatchWriter createBatchWriter(String stream, String contentType) throws IOException {
    return streamWriter.createBatchWriter(stream, contentType);
  }
}
