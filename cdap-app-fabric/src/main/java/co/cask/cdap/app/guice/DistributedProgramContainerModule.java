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

package co.cask.cdap.app.guice;

import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DFSLocationModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.guice.ZKDiscoveryModule;
import co.cask.cdap.common.namespace.NamespacePathLocator;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NoLookupNamespacePathLocator;
import co.cask.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.store.NoOpMetadataStore;
import co.cask.cdap.data2.metadata.writer.FieldLineageWriter;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.metadata.writer.MessagingLineageWriter;
import co.cask.cdap.data2.registry.MessagingUsageWriter;
import co.cask.cdap.data2.registry.UsageWriter;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.client.ProgramDiscoveryExploreClient;
import co.cask.cdap.internal.app.program.MessagingProgramStateWriter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitorServer;
import co.cask.cdap.internal.app.runtime.workflow.MessagingWorkflowStateWriter;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import co.cask.cdap.logging.guice.KafkaLogAppenderModule;
import co.cask.cdap.logging.guice.TMSLogAppenderModule;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metadata.MetadataReaderWriterModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.guice.SecureStoreClientModule;
import co.cask.cdap.security.impersonation.CurrentUGIProvider;
import co.cask.cdap.security.impersonation.NoOpOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.tools.KeyStores;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ServiceAnnouncer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * The guice {@link Module} used for program execution in distributed mode. This module
 * is used in both client/driver containers and task containers.
 */
public class DistributedProgramContainerModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedProgramContainerModule.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final ProgramRunId programRunId;
  private final Arguments systemArgs;
  @Nullable
  private final ServiceAnnouncer serviceAnnouncer;

  public DistributedProgramContainerModule(CConfiguration cConf, Configuration hConf,
                                           ProgramRunId programRunId, Arguments systemArgs) {
    this(cConf, hConf, programRunId, systemArgs, null);
  }

  public DistributedProgramContainerModule(CConfiguration cConf, Configuration hConf, ProgramRunId programRunId,
                                           Arguments systemArgs, @Nullable ServiceAnnouncer serviceAnnouncer) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.programRunId = programRunId;
    this.systemArgs = systemArgs;
    this.serviceAnnouncer = serviceAnnouncer;
  }

  @Override
  protected void configure() {
    List<Module> modules = getCoreModules(programRunId.getParent());
    String principal = systemArgs.getOption(ProgramOptionConstants.PRINCIPAL);

    AuthenticationContextModules authModules = new AuthenticationContextModules();
    modules.add(principal == null
                  ? authModules.getProgramContainerModule()
                  : authModules.getProgramContainerModule(principal));

    install(Modules.override(modules).with(new AbstractModule() {
      @Override
      protected void configure() {
        // Overrides the LineageWriter, UsageWriter to write to TMS instead
        bind(LineageWriter.class).to(MessagingLineageWriter.class);
        bind(FieldLineageWriter.class).to(MessagingLineageWriter.class);
        bind(UsageWriter.class).to(MessagingUsageWriter.class);
        // Overrides the metadata store to be no-op (programs never access it directly)
        bind(MetadataStore.class).to(NoOpMetadataStore.class);
      }
    }));
  }

  private List<Module> getCoreModules(final ProgramId programId) {
    ClusterMode clusterMode = systemArgs.hasOption(ProgramOptionConstants.CLUSTER_MODE)
      ? ClusterMode.valueOf(systemArgs.getOption(ProgramOptionConstants.CLUSTER_MODE))
      : ClusterMode.ON_PREMISE;

    List<Module> modules = new ArrayList<>();

    modules.add(new ConfigModule(cConf, hConf));
    modules.add(new IOModule());
    modules.add(new ZKClientModule());
    modules.add(new ZKDiscoveryModule());
    modules.add(new MetricsClientRuntimeModule().getDistributedModules());
    modules.add(new MessagingClientModule());
    modules.add(new AuditModule());
    modules.add(new AuthorizationEnforcementModule().getDistributedModules());
    modules.add(new SecureStoreClientModule());
    modules.add(new MetadataReaderWriterModules().getDistributedModules());
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
        bind(ProgramId.class).toInstance(programId);

        if (serviceAnnouncer != null) {
          bind(ServiceAnnouncer.class).toInstance(serviceAnnouncer);
        }
      }
    });

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

  private void addOnPremiseModules(List<Module> modules) {
    String instanceId = systemArgs.getOption(ProgramOptionConstants.INSTANCE_ID);

    modules.add(new DFSLocationModule());
    modules.add(new KafkaClientModule());
    modules.add(new KafkaLogAppenderModule());
    modules.add(new DataFabricModules(generateClientId(programRunId, instanceId)).getDistributedModules());
    modules.add(new DataSetsModules().getDistributedModules());
    modules.add(new NamespaceQueryAdminModule());
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        // bind explore client to ProgramDiscoveryExploreClient which is aware of the programId
        bind(ExploreClient.class).to(ProgramDiscoveryExploreClient.class).in(Scopes.SINGLETON);
      }
    });
  }

  private void addIsolatedModules(List<Module> modules) {
    modules.add(new TMSLogAppenderModule());
    modules.add(new DFSLocationModule());
    modules.add(new DataSetsModules().getStandaloneModules());
    modules.add(new DataSetServiceModules().getStandaloneModules());
    modules.add(Modules.override(new DataFabricModules().getInMemoryModules()).with(new AbstractModule() {
      @Override
      protected void configure() {
        // Use the ConstantTransactionSystemClient in isolated mode, basically there is no transaction.
        bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class).in(Scopes.SINGLETON);
      }
    }));

    // In isolated mode, ignore the namespace mapping
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(NamespacePathLocator.class).to(NoLookupNamespacePathLocator.class);
      }
    });

    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        // Bind to unsupported/no-op class implementations for features that are not supported in isolated cluster mode
        bind(ExploreClient.class).to(UnsupportedExploreClient.class);
        bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);

        // This is just for Dataset Service to check if a namespace exists
        bind(NamespaceQueryAdmin.class).toInstance(new NamespaceQueryAdmin() {
          @Override
          public List<NamespaceMeta> list() {
            return Collections.singletonList(get(programRunId.getNamespaceId()));
          }

          @Override
          public NamespaceMeta get(NamespaceId namespaceId) {
            return new NamespaceMeta.Builder().setName(namespaceId).build();
          }

          @Override
          public boolean exists(NamespaceId namespaceId) {
            return programRunId.getNamespaceId().equals(namespaceId);
          }
        });

        bindRuntimeMonitorServer(binder());
      }
    });
  }

  /**
   * Optionally adds {@link RuntimeMonitorServer} binding.
   */
  private void bindRuntimeMonitorServer(Binder binder) {
    try {
      Path keyStorePath = Paths.get(Constants.RuntimeMonitor.SERVER_KEYSTORE);
      Path trustStorePath = Paths.get(Constants.RuntimeMonitor.CLIENT_KEYSTORE);

      // If there is no key store or trust store, don't add the binding.
      // The reason is that this module is used in all containers, but only the driver container would have the
      // key store files.
      if (!Files.isReadable(keyStorePath) || !Files.isReadable(trustStorePath)) {
        return;
      }

      KeyStore keyStore = loadKeyStore(keyStorePath);
      KeyStore trustStore = loadKeyStore(trustStorePath);

      binder.install(new PrivateModule() {
        @Override
        protected void configure() {
          bind(KeyStore.class).annotatedWith(Constants.AppFabric.KeyStore.class).toInstance(keyStore);
          bind(KeyStore.class).annotatedWith(Constants.AppFabric.TrustStore.class).toInstance(trustStore);
          bind(RuntimeMonitorServer.class);
          expose(RuntimeMonitorServer.class);
        }
      });

    } catch (Exception e) {
      // Just log if failed to load the KeyStores. It will fail when RuntimeMonitorServer is needed.
      LOG.error("Failed to load key store and/or trust store", e);
    }
  }

  /**
   * Loads a {@link KeyStore} by reading the given file path.
   */
  private KeyStore loadKeyStore(Path keyStorePath) throws Exception {
    KeyStore keyStore = KeyStore.getInstance(KeyStores.SSL_KEYSTORE_TYPE);
    try (InputStream is = Files.newInputStream(keyStorePath)) {
      keyStore.load(is, "".toCharArray());
      return keyStore;
    }
  }

  private static String generateClientId(ProgramRunId programRunId, String instanceId) {
    return String.format("%s.%s.%s", programRunId.getParent(), programRunId.getRun(), instanceId);
  }
}
