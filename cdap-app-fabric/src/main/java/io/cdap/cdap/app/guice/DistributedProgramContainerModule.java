/*
 * Copyright © 2018-2023 Cask Data, Inc.
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

package io.cdap.cdap.app.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.auditlogging.AuditLogPublisherService;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.guice.ZkClientModule;
import io.cdap.cdap.common.guice.ZkDiscoveryModule;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import io.cdap.cdap.data.runtime.ConstantTransactionSystemClient;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.LineageWriter;
import io.cdap.cdap.data2.metadata.writer.MessagingLineageWriter;
import io.cdap.cdap.data2.registry.MessagingUsageWriter;
import io.cdap.cdap.data2.registry.UsageWriter;
import io.cdap.cdap.internal.app.program.MessagingProgramStatePublisher;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.program.ProgramStatePublisher;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitors;
import io.cdap.cdap.internal.app.runtime.workflow.MessagingWorkflowStateWriter;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import io.cdap.cdap.logging.guice.KafkaLogAppenderModule;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.logging.guice.TMSLogAppenderModule;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.messaging.client.ClientMessagingService;
import io.cdap.cdap.messaging.guice.MessagingServiceModule;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.metadata.RemotePreferencesFetcherInternal;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.auth.service.DefaultAuditLogPublisherService;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.CoreSecurityModule;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.guice.SecureStoreClientModule;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.NoOpOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.noop.NoopMetadataStorage;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * The guice {@link Module} used for program execution in distributed mode. This module is used in
 * both client/driver containers and task containers.
 */
public class DistributedProgramContainerModule extends AbstractModule {

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final ProgramRunId programRunId;
  private final ProgramOptions programOpts;
  @Nullable
  private final ServiceAnnouncer serviceAnnouncer;

  public DistributedProgramContainerModule(CConfiguration cConf, Configuration hConf,
      ProgramRunId programRunId, ProgramOptions programOpts) {
    this(cConf, hConf, programRunId, programOpts, null);
  }

  /**
   * Creates a program container module for a program.
   *
   * @param cConf            The CConf to use.
   * @param hConf            The HConf to use.
   * @param programRunId     The program run ID.
   * @param programOpts      The program options.
   * @param serviceAnnouncer The service announcer to use.
   */
  public DistributedProgramContainerModule(CConfiguration cConf, Configuration hConf,
      ProgramRunId programRunId,
      ProgramOptions programOpts, @Nullable ServiceAnnouncer serviceAnnouncer) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.programRunId = programRunId;
    this.programOpts = programOpts;
    this.serviceAnnouncer = serviceAnnouncer;
  }

  @Override
  protected void configure() {
    List<Module> modules = getCoreModules();

    RuntimeMonitorType runtimeMonitorType = SystemArguments.getRuntimeMonitorType(cConf,
        programOpts);
    modules.add(RuntimeMonitors.getRemoteAuthenticatorModule(runtimeMonitorType, programOpts));

    install(Modules.override(modules).with(new AbstractModule() {
      @Override
      protected void configure() {
        // Overrides the LineageWriter, UsageWriter to write to TMS instead
        bind(LineageWriter.class).to(MessagingLineageWriter.class);
        bind(FieldLineageWriter.class).to(MessagingLineageWriter.class);
        bind(UsageWriter.class).to(MessagingUsageWriter.class);
        // Overrides the metadata store to be no-op (programs never access it directly)
        bind(MetadataStorage.class).to(NoopMetadataStorage.class);
      }
    }));

    bind(RuntimeMonitorType.class).toInstance(runtimeMonitorType);
  }

  private List<Module> getCoreModules() {
    Arguments systemArgs = programOpts.getArguments();
    ClusterMode clusterMode = ProgramRunners.getClusterMode(programOpts);

    List<Module> modules = new ArrayList<>();

    modules.add(new ConfigModule(cConf, hConf));
    modules.add(new IOModule());
    modules.add(new DFSLocationModule());
    modules.add(new MetricsClientRuntimeModule().getDistributedModules());
    modules.add(new MessagingServiceModule(cConf));
    modules.add(new AuditModule());
    modules.add(new AuthorizationEnforcementModule().getDistributedModules());
    modules.add(new SecureStoreClientModule());
    modules.add(new MetadataReaderWriterModules().getDistributedModules());
    modules.add(new AppStateModule());
    modules.add(new NamespaceQueryAdminModule());
    modules.add(new DataSetsModules().getDistributedModules());
    modules.add(new ProgramStateWriterModule(clusterMode,
        systemArgs.hasOption(ProgramOptionConstants.PEER_NAME)));
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(WorkflowStateWriter.class).to(MessagingWorkflowStateWriter.class);

        // don't need to perform any impersonation from within user programs
        bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);

        // Bind ProgramId to the passed in instance programId so that we can retrieve it back later when needed.
        // Also binding to instance is fine here as the programId is guaranteed to not change throughout the
        // lifecycle of this program runnable
        bind(ProgramId.class).toInstance(programRunId.getParent());
        bind(ProgramRunId.class).toInstance(programRunId);

        if (serviceAnnouncer != null) {
          bind(ServiceAnnouncer.class).toInstance(serviceAnnouncer);
        }

        bind(PreferencesFetcher.class).to(RemotePreferencesFetcherInternal.class)
            .in(Scopes.SINGLETON);
//        bind(AuditLogPublisherService.class).to(DefaultAuditLogPublisherService.class);
      }
    });

    addDataFabricModules(modules);

    switch (clusterMode) {
      case ON_PREMISE:
        addOnPremiseModules(modules);
        break;
      case ISOLATED:
        addIsolatedModules(modules);
        break;
      default:
        // This shouldn't happen. Just don't add any extra modules.
    }

    return modules;
  }

  /**
   * Adds guice modules that are related to data fabric.
   */
  private void addDataFabricModules(List<Module> modules) {
    if (cConf.getBoolean(Constants.Transaction.TX_ENABLED)) {
      String instanceId = programOpts.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID);
      modules.add(new DataFabricModules(
          generateClientId(programRunId, instanceId)).getDistributedModules());
      modules.add(new SystemDatasetRuntimeModule().getDistributedModules());
    } else {
      modules.add(new DataSetServiceModules().getStandaloneModules());
      modules.add(
          Modules.override(new DataFabricModules().getInMemoryModules()).with(new AbstractModule() {
            @Override
            protected void configure() {
              // Use the ConstantTransactionSystemClient in isolated mode, basically there is no transaction.
              bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class)
                  .in(Scopes.SINGLETON);
            }
          }));
    }
  }

  private void addOnPremiseModules(List<Module> modules) {
    CoreSecurityModule coreSecurityModule = CoreSecurityRuntimeModule.getDistributedModule(cConf);
    modules.add(new AuthenticationContextModules().getMasterModule());
    modules.add(coreSecurityModule);

    // If MasterEnvironment is not available, assuming it is the old hadoop stack with ZK, Kafka
    MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();

    if (masterEnv == null) {
      modules.add(new ZkClientModule());
      modules.add(new ZkDiscoveryModule());
      modules.add(new KafkaClientModule());
      modules.add(new KafkaLogAppenderModule());
      return;
    }

    if (coreSecurityModule.requiresZKClient()) {
      modules.add(new ZkClientModule());
    }

    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(DiscoveryService.class)
            .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
        bind(DiscoveryServiceClient.class)
            .toProvider(
                new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));

        bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
      }
    });
    modules.add(new RemoteLogAppenderModule());
  }

  private void addIsolatedModules(List<Module> modules) {
    modules.add(new RemoteExecutionDiscoveryModule());
    // Use RemoteLogAppender if we're running in tethered mode so that logs get written to the log saver.
    // Otherwise write to the TMSLogAppender, will be consumed by RuntimeClientService.
    if (programOpts.getArguments().getOption(ProgramOptionConstants.PEER_NAME) != null) {
      modules.add(new RemoteLogAppenderModule());
    } else {
      modules.add(new TMSLogAppenderModule());
    }
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);
      }
    });

    // For execution within a remote cluster we use a token from a file passed to
    // the driver or configuration passed from driver to workers, see
    // io.cdap.cdap.security.auth.context.AuthenticationContextModules.loadRemoteCredentials
    AuthenticationContextModules authModules = new AuthenticationContextModules();
    String principal = programOpts.getArguments().getOption(ProgramOptionConstants.PRINCIPAL);
    if (principal == null) {
      modules.add(authModules.getProgramContainerModule(cConf));
    } else {
      modules.add(authModules.getProgramContainerModule(cConf, principal));
    }
  }

  private static String generateClientId(ProgramRunId programRunId, String instanceId) {
    return String.format("%s.%s.%s", programRunId.getParent(), programRunId.getRun(), instanceId);
  }

  /**
   * A Guice module to provide {@link ProgramStateWriter} binding. In normal same cluster / remote
   * cluster execution, it just publishes program states to TMS based on the discovery service. For
   * tethered execution, it publishes program states to the TMS running in the current master
   * environment, instead of using the normal discovery service that bind to talk to the originating
   * CDAP instance.
   */
  private static final class ProgramStateWriterModule extends PrivateModule {

    private final ClusterMode clusterMode;
    private final boolean tethered;

    private ProgramStateWriterModule(ClusterMode clusterMode, boolean tethered) {
      this.clusterMode = clusterMode;
      this.tethered = tethered;
    }

    @Override
    protected void configure() {
      MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();

      if (clusterMode == ClusterMode.ISOLATED && tethered && masterEnv != null) {
        bind(MasterEnvironment.class).toInstance(masterEnv);
        bind(ProgramStatePublisher.class).toProvider(ProgramStatePublisherProvider.class);
      } else {
        bind(ProgramStatePublisher.class).to(MessagingProgramStatePublisher.class);
      }

      bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class);
      expose(ProgramStateWriter.class);
    }
  }

  /**
   * A guice {@link Provider} for providing the binding for {@link ProgramStatePublisher} that is
   * used in tethered execution.
   */
  private static final class ProgramStatePublisherProvider implements
      Provider<ProgramStatePublisher> {

    private final CConfiguration cConf;
    private final MasterEnvironment masterEnv;
    private final InternalAuthenticator internalAuthenticator;

    @Inject
    ProgramStatePublisherProvider(CConfiguration cConf, MasterEnvironment masterEnv,
        InternalAuthenticator internalAuthenticator) {
      this.cConf = cConf;
      this.masterEnv = masterEnv;
      this.internalAuthenticator = internalAuthenticator;
    }

    @Override
    public ProgramStatePublisher get() {
      RemoteClientFactory remoteClientFactory = new RemoteClientFactory(
          masterEnv.getDiscoveryServiceClientSupplier().get(),
          internalAuthenticator);

      return new MessagingProgramStatePublisher(cConf,
          new ClientMessagingService(cConf, remoteClientFactory));
    }
  }
}
