/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.test.internal;

import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.data.dataset.DatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.AppFabricClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.DefaultMapReduceManager;
import co.cask.cdap.test.DefaultSparkManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * A default implementation of {@link ApplicationManager}.
 */
public class DefaultApplicationManager implements ApplicationManager {

  private final ConcurrentMap<String, Id.Program> runningProcesses = Maps.newConcurrentMap();
  private final Id.Application applicationId;
  private final TransactionSystemClient txSystemClient;
  private final DatasetInstantiator datasetInstantiator;
  private final StreamWriterFactory streamWriterFactory;
  private final AppFabricClient appFabricClient;
  private final DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public DefaultApplicationManager(DatasetFramework datasetFramework,
                                   TransactionSystemClient txSystemClient,
                                   StreamWriterFactory streamWriterFactory,
                                   DiscoveryServiceClient discoveryServiceClient,
                                   TemporaryFolder tempFolder,
                                   AppFabricClient appFabricClient,
                                   @Assisted("applicationId") Id.Application applicationId,
                                   @Assisted Location deployedJar) {
    this.applicationId = applicationId;
    this.streamWriterFactory = streamWriterFactory;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txSystemClient = txSystemClient;
    this.appFabricClient = appFabricClient;

    try {
      File tempDir = tempFolder.newFolder();
      BundleJarUtil.unpackProgramJar(deployedJar, tempDir);
      ClassLoader classLoader = ProgramClassLoader.create(tempDir, getClass().getClassLoader());
      this.datasetInstantiator = new DatasetInstantiator(applicationId.getNamespace(),
                                                         datasetFramework,
                                                         new DataSetClassLoader(classLoader),
                                                         Collections.singleton(applicationId),
                                                         // todo: collect metrics for datasets outside programs too
                                                         null);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private static final class DataSetClassLoader extends ClassLoader {

    private final ClassLoader classLoader;

    private DataSetClassLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      return classLoader.loadClass(name);
    }
  }

  @Override
  public FlowManager startFlow(final String flowName) {
    return startFlow(flowName, ImmutableMap.<String, String>of());
  }

  @Override
  public FlowManager startFlow(final String flowName, Map<String, String> arguments) {
    final Id.Program flowId = startProgram(flowName, arguments, ProgramType.FLOW);
    return new DefaultFlowManager(flowId, appFabricClient, this);
  }

  @Override
  public MapReduceManager startMapReduce(final String programName) {
    return startMapReduce(programName, ImmutableMap.<String, String>of());
  }

  @Override
  public MapReduceManager startMapReduce(final String programName, Map<String, String> arguments) {
    return getMapReduceManager(programName, arguments, ProgramType.MAPREDUCE);
  }

  private MapReduceManager getMapReduceManager(final String programName, Map<String, String> arguments,
                                               final ProgramType programType) {
    try {
      final Id.Program programId = startProgram(programName, arguments, programType);
      return new DefaultMapReduceManager(programId, this);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public SparkManager startSpark(String programName) {
    return startSpark(programName, ImmutableMap.<String, String>of());
  }

  @Override
  public SparkManager startSpark(String programName, Map<String, String> arguments) {
    return getSparkManager(programName, arguments, ProgramType.SPARK);
  }

  private SparkManager getSparkManager(final String programName, Map<String, String> arguments,
                                       final ProgramType programType) {
    try {
      final Id.Program jobId = startProgram(programName, arguments, programType);
      return new DefaultSparkManager(jobId, this);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private Id.Program startProgram(String programName, Map<String, String> arguments, ProgramType programType) {
    final Id.Program programId = Id.Program.from(applicationId, programType, programName);
    // program can stop by itself, so refreshing info about its state
    if (!isRunning(programId)) {
      runningProcesses.remove(programName);
    }

    Preconditions.checkState(runningProcesses.putIfAbsent(programName, programId) == null,
                             programType + " program %s is already running", programName);
    try {
      appFabricClient.startProgram(applicationId.getNamespaceId(), applicationId.getId(),
                                   programName, programType, arguments);
    } catch (Exception e) {
      runningProcesses.remove(programName);
      throw Throwables.propagate(e);
    }
    return programId;
  }

  @Override
  public WorkflowManager startWorkflow(final String workflowName, Map<String, String> arguments) {
    final Id.Program workflowId = Id.Program.from(applicationId, ProgramType.WORKFLOW, workflowName);
    // currently we are using it for schedule, so not starting the workflow
    return new DefaultWorkflowManager(workflowId, appFabricClient, this);
  }

  @Override
  public ServiceManager startService(String serviceName) {
    return startService(serviceName, ImmutableMap.<String, String>of());
  }

  @Override
  public ServiceManager startService(final String serviceName, Map<String, String> arguments) {
    final Id.Program serviceId = startProgram(serviceName, arguments, ProgramType.SERVICE);
    return new DefaultServiceManager(serviceId, appFabricClient, discoveryServiceClient, this);
  }

  @Override
  public WorkerManager startWorker(String workerName) {
    return startWorker(workerName, ImmutableMap.<String, String>of());
  }

  @Override
  public WorkerManager startWorker(String workerName, Map<String, String> arguments) {
    final Id.Program workerId = startProgram(workerName, arguments, ProgramType.WORKER);
    return new DefaultWorkerManager(workerId, appFabricClient, this);
  }

  @Override
  @Deprecated
  public StreamWriter getStreamWriter(String streamName) {
    Id.Stream streamId = Id.Stream.from(applicationId.getNamespace(), streamName);
    return streamWriterFactory.create(streamId);
  }

  @Override
  public <T> DataSetManager<T> getDataSet(String dataSetName) {
    @SuppressWarnings("unchecked")
    final T dataSet = (T) datasetInstantiator.getDataset(dataSetName);

    try {
      final TransactionContext txContext =
        new TransactionContext(txSystemClient, datasetInstantiator.getTransactionAware());
      txContext.start();
      return new DataSetManager<T>() {
        @Override
        public T get() {
          return dataSet;
        }

        @Override
        public void flush() {
          try {
            txContext.finish();
            txContext.start();
          } catch (TransactionFailureException e) {
            throw Throwables.propagate(e);
          }
        }
      };
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stopAll() {
    try {
      for (Map.Entry<String, Id.Program> entry : Iterables.consumingIterable(runningProcesses.entrySet())) {
        // have to do a check, since mapreduce jobs could stop by themselves earlier, and appFabricServer.stop will
        // throw error when you stop something that is not running.
        if (isRunning(entry.getValue())) {
          Id.Program id = entry.getValue();
          appFabricClient.stopProgram(applicationId.getNamespaceId(), id.getApplicationId(),
                                      id.getId(), id.getType());
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void stopProgram(Id.Program programId) {
    String programName = programId.getId();
    try {
      if (runningProcesses.remove(programName, programId)) {
        appFabricClient.stopProgram(applicationId.getNamespaceId(), applicationId.getId(),
                                    programName, programId.getType());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isRunning(Id.Program programId) {
    try {
      String status = appFabricClient.getStatus(applicationId.getNamespaceId(), programId.getApplicationId(),
                                                programId.getId(), programId.getType());
      // comparing to hardcoded string is ugly, but this is how appFabricServer works now to support legacy UI
      return "STARTING".equals(status) || "RUNNING".equals(status);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
