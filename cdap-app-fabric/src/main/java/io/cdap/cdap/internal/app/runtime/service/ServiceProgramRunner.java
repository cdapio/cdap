/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.service;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.data.ProgramContextAware;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AbstractProgramRunnerWithPlugin;
import io.cdap.cdap.internal.app.runtime.BasicProgramContext;
import io.cdap.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.services.ServiceHttpServer;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Collections;

/**
 * A {@link ProgramRunner} that runs an HTTP Server inside a Service.
 */
public class ServiceProgramRunner extends AbstractProgramRunnerWithPlugin {

  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final ServiceAnnouncer serviceAnnouncer;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;
  private final MessagingService messagingService;
  private final ArtifactManagerFactory artifactManagerFactory;
  private final MetadataReader metadataReader;
  private final MetadataPublisher metadataPublisher;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final PluginFinder pluginFinder;
  private final TransactionRunner transactionRunner;
  private final FieldLineageWriter fieldLineageWriter;

  @Inject
  public ServiceProgramRunner(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                              DatasetFramework datasetFramework, DiscoveryServiceClient discoveryServiceClient,
                              TransactionSystemClient txClient, ServiceAnnouncer serviceAnnouncer,
                              SecureStore secureStore, SecureStoreManager secureStoreManager,
                              MessagingService messagingService,
                              ArtifactManagerFactory artifactManagerFactory,
                              MetadataReader metadataReader, MetadataPublisher metadataPublisher,
                              NamespaceQueryAdmin namespaceQueryAdmin, PluginFinder pluginFinder,
                              TransactionRunner transactionRunner, FieldLineageWriter fieldLineageWriter) {
    super(cConf);
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
    this.serviceAnnouncer = serviceAnnouncer;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
    this.messagingService = messagingService;
    this.artifactManagerFactory = artifactManagerFactory;
    this.metadataReader = metadataReader;
    this.metadataPublisher = metadataPublisher;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.pluginFinder = pluginFinder;
    this.transactionRunner = transactionRunner;
    this.fieldLineageWriter = fieldLineageWriter;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    int instanceId = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID, "-1"));
    Preconditions.checkArgument(instanceId >= 0, "Missing instance Id");

    int instanceCount = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCES, "0"));
    Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

    RunId runId = ProgramRunners.getRunId(options);

    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType programType = program.getType();
    Preconditions.checkNotNull(programType, "Missing processor type.");
    Preconditions.checkArgument(programType == ProgramType.SERVICE, "Only Service process type is supported.");

    ServiceSpecification spec = appSpec.getServices().get(program.getName());

    String host = options.getArguments().getOption(ProgramOptionConstants.HOST);
    Preconditions.checkArgument(host != null, "No hostname is provided");

    // Setup dataset framework context, if required
    if (datasetFramework instanceof ProgramContextAware) {
      ProgramId programId = program.getId();
      ((ProgramContextAware) datasetFramework).setContext(new BasicProgramContext(programId.run(runId)));
    }

    final PluginInstantiator pluginInstantiator = createPluginInstantiator(options, program.getClassLoader());
    try {
      RetryStrategy retryStrategy = SystemArguments.getRetryStrategy(options.getUserArguments().asMap(),
                                                                     program.getType(), cConf);
      ArtifactManager artifactManager = artifactManagerFactory.create(program.getId().getNamespaceId(), retryStrategy);
      ServiceHttpServer component = new ServiceHttpServer(host, program, options, cConf, spec,
                                                          instanceId, instanceCount, serviceAnnouncer,
                                                          metricsCollectionService, datasetFramework,
                                                          txClient, discoveryServiceClient,
                                                          pluginInstantiator, secureStore, secureStoreManager,
                                                          messagingService, artifactManager, metadataReader,
                                                          metadataPublisher, namespaceQueryAdmin, pluginFinder,
                                                          transactionRunner, fieldLineageWriter);

      // Add a service listener to make sure the plugin instantiator is closed when the http server is finished.
      component.addListener(createRuntimeServiceListener(Collections.singleton(pluginInstantiator)),
                                                         Threads.SAME_THREAD_EXECUTOR);

      ProgramController controller = new ServiceProgramControllerAdapter(component, program.getId().run(runId));
      component.start();
      return controller;
    } catch (Throwable t) {
      Closeables.closeQuietly(pluginInstantiator);
      throw t;
    }
  }

  private static final class ServiceProgramControllerAdapter extends ProgramControllerServiceAdapter {
    private final ServiceHttpServer service;

    ServiceProgramControllerAdapter(ServiceHttpServer service, ProgramRunId programRunId) {
      super(service, programRunId);
      this.service = service;
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      super.doCommand(name, value);
      if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Integer)) {
        return;
      }
      service.setInstanceCount((Integer) value);
    }
  }
}
