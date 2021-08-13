/*
 * Copyright © 2018-2019 Cask Data, Inc.
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
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
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
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
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
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.explore.client.ProgramDiscoveryExploreClient;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.workflow.MessagingWorkflowStateWriter;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import io.cdap.cdap.logging.guice.KafkaLogAppenderModule;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.logging.guice.TMSLogAppenderModule;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.SparkConfigs;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.metadata.RemotePreferencesFetcherInternal;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * The guice {@link Module} used for program execution in distributed mode. This module
 * is used in both client/driver containers and task containers.
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

  public DistributedProgramContainerModule(CConfiguration cConf, Configuration hConf, ProgramRunId programRunId,
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

    bind(RuntimeMonitorType.class).toInstance(SystemArguments.getRuntimeMonitorType(cConf, programOpts));
  }

  private List<Module> getCoreModules() {
    Arguments systemArgs = programOpts.getArguments();
    ClusterMode clusterMode = systemArgs.hasOption(ProgramOptionConstants.CLUSTER_MODE)
      ? ClusterMode.valueOf(systemArgs.getOption(ProgramOptionConstants.CLUSTER_MODE))
      : ClusterMode.ON_PREMISE;

    List<Module> modules = new ArrayList<>();

    modules.add(new ConfigModule(cConf, hConf));
    modules.add(new IOModule());
    modules.add(new DFSLocationModule());
    modules.add(new MetricsClientRuntimeModule().getDistributedModules());
    modules.add(new MessagingClientModule());
    modules.add(new AuditModule());
    modules.add(new AuthorizationEnforcementModule().getDistributedModules());
    modules.add(new SecureStoreClientModule());
    modules.add(new MetadataReaderWriterModules().getDistributedModules());
    modules.add(new NamespaceQueryAdminModule());
    modules.add(new DataSetsModules().getDistributedModules());
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class);
        bind(WorkflowStateWriter.class).to(MessagingWorkflowStateWriter.class);

        // don't need to perform any impersonation from within user programs
        bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);

        // Bind ProgramId to the passed in instance programId so that we can retrieve it back later when needed.
        // For example see ProgramDiscoveryExploreClient.
        // Also binding to instance is fine here as the programId is guaranteed to not change throughout the
        // lifecycle of this program runnable
        bind(ProgramId.class).toInstance(programRunId.getParent());
        bind(ProgramRunId.class).toInstance(programRunId);

        if (serviceAnnouncer != null) {
          bind(ServiceAnnouncer.class).toInstance(serviceAnnouncer);
        }

        bind(PreferencesFetcher.class).to(RemotePreferencesFetcherInternal.class).in(Scopes.SINGLETON);
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
      modules.add(new DataFabricModules(generateClientId(programRunId, instanceId)).getDistributedModules());
      modules.add(new SystemDatasetRuntimeModule().getDistributedModules());
    } else {
      modules.add(new DataSetServiceModules().getStandaloneModules());
      modules.add(Modules.override(new DataFabricModules().getInMemoryModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          // Use the ConstantTransactionSystemClient in isolated mode, basically there is no transaction.
          bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class).in(Scopes.SINGLETON);
        }
      }));
    }

    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      modules.add(new AbstractModule() {
        @Override
        protected void configure() {
          // bind explore client to ProgramDiscoveryExploreClient which is aware of the programId
          bind(ExploreClient.class).to(ProgramDiscoveryExploreClient.class).in(Scopes.SINGLETON);
        }
      });
    } else {
      modules.add(new AbstractModule() {
        @Override
        protected void configure() {
          // Bind to unsupported/no-op class implementations
          bind(ExploreClient.class).to(UnsupportedExploreClient.class);
        }
      });
    }
  }

  private void addOnPremiseModules(List<Module> modules) {
    CoreSecurityModule coreSecurityModule = CoreSecurityRuntimeModule.getDistributedModule(cConf);
    modules.add(new AuthenticationContextModules().getMasterModule());
    modules.add(coreSecurityModule);

    // If MasterEnvironment is not available, assuming it is the old hadoop stack with ZK, Kafka
    MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();

    if (masterEnv == null) {
      modules.add(new ZKClientModule());
      modules.add(new ZKDiscoveryModule());
      modules.add(new KafkaClientModule());
      modules.add(new KafkaLogAppenderModule());
      return;
    } else {
      System.err.println("### Master env is not null");
      modules.add(new AbstractModule() {
        @Override
        protected void configure() {
          // get spark confs
          bind(SparkConfigs.class).toInstance(masterEnv.getSparkConf());
        }
      });
    }

    if (coreSecurityModule.requiresZKClient()) {
      modules.add(new ZKClientModule());
    }

    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(DiscoveryService.class)
          .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
        bind(DiscoveryServiceClient.class)
          .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));
        // get spark confs
        bind(SparkConfigs.class).toInstance(masterEnv.getSparkConf());
        bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
      }
    });
    modules.add(new RemoteLogAppenderModule());
  }

  private void addIsolatedModules(List<Module> modules) {
    modules.add(new RemoteExecutionDiscoveryModule());
    modules.add(new TMSLogAppenderModule());
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
}
