/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

import co.cask.cdap.api.artifact.ArtifactManager;
import co.cask.cdap.api.data.stream.StreamWriter;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.app.stream.DefaultStreamWriter;
import co.cask.cdap.app.stream.StreamWriterFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.metadata.writer.MessagingLineageWriter;
import co.cask.cdap.data2.registry.RuntimeUsageRegistry;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.client.ProgramDiscoveryExploreClient;
import co.cask.cdap.internal.app.program.MessagingProgramStateWriter;
import co.cask.cdap.internal.app.queue.QueueReaderFactory;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import co.cask.cdap.internal.app.runtime.artifact.PluginFinder;
import co.cask.cdap.internal.app.runtime.artifact.RemoteArtifactManager;
import co.cask.cdap.internal.app.runtime.artifact.RemotePluginFinder;
import co.cask.cdap.internal.app.runtime.batch.MapReduceProgramRunner;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramRunner;
import co.cask.cdap.internal.app.store.remote.RemoteRuntimeStore;
import co.cask.cdap.internal.app.store.remote.RemoteRuntimeUsageRegistry;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.RemotePrivilegesManager;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.security.impersonation.CurrentUGIProvider;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ServiceAnnouncer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Defines guice modules for distributed program containers, include client containers
 * (e.g. {@link MapReduceProgramRunner}, {@link WorkflowProgramRunner}) as well as task containers.
 */
public class DistributedProgramRunnableModule {

  private final CConfiguration cConf;
  private final Configuration hConf;

  public DistributedProgramRunnableModule(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
  }

  /**
   * Creates a guice {@link Module} that contains all bindings needed for the CDAP runtime environment,
   * except for service announcement.
   *
   * @param programRunId the {@link ProgramRunId} of the program run
   * @param instanceId the id for the current container instance
   * @param principal an principal of the application owner
   * @return a guice {@link Module}
   */
  public Module createModule(ProgramRunId programRunId, String instanceId, @Nullable String principal) {
    return createModule(programRunId, instanceId, principal, null);
  }

  /**
   * Creates a guice {@link Module} that contains all bindings needed for the CDAP runtime environment.
   *
   * @param programRunId the {@link ProgramRunId} of the program run
   * @param instanceId the id for the current container instance
   * @param principal an principal of the application owner
   * @param serviceAnnouncer the {@link ServiceAnnouncer} to use in the binding.
   * @return a guice {@link Module}
   */
  public Module createModule(ProgramRunId programRunId, String instanceId,
                             @Nullable String principal, @Nullable ServiceAnnouncer serviceAnnouncer) {
    String txClientId = generateClientId(programRunId, instanceId);
    List<Module> modules = getCoreModules(programRunId.getParent(), txClientId);

    AuthenticationContextModules authModules = new AuthenticationContextModules();
    modules.add(principal == null
                  ? authModules.getProgramContainerModule()
                  : authModules.getProgramContainerModule(principal));

    return Modules.override(modules).with(new AbstractModule() {
      @Override
      protected void configure() {
        bind(LineageWriter.class).to(MessagingLineageWriter.class);
        bind(RuntimeUsageRegistry.class).to(RemoteRuntimeUsageRegistry.class).in(Scopes.SINGLETON);
        if (serviceAnnouncer != null) {
          bind(ServiceAnnouncer.class).toInstance(serviceAnnouncer);
        }
      }
    });
  }

  private List<Module> getCoreModules(final ProgramId programId, String txClientId) {
    return new ArrayList<>(Arrays.<Module>asList(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new MessagingClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new LoggingModules().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataFabricModules(txClientId).getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new ViewAdminModules().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NotificationFeedClientModule(),
      new AuditModule().getDistributedModules(),
      new NamespaceClientRuntimeModule().getDistributedModules(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new SecureStoreModules().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // For Binding queue stuff
          bind(QueueReaderFactory.class).in(Scopes.SINGLETON);

          // For binding DataSet transaction stuff
          install(new DataFabricFacadeModule());

          bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class);

          bind(RuntimeStore.class).to(RemoteRuntimeStore.class);

          // For binding StreamWriter
          install(createStreamFactoryModule());

          // don't need to perform any impersonation from within user programs
          bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);

          // bind PrivilegesManager to a remote implementation, so it does not need to instantiate the authorizer
          bind(PrivilegesManager.class).to(RemotePrivilegesManager.class);

          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);

          // Bind ProgramId to the passed in instance programId so that we can retrieve it back later when needed.
          // For example see ProgramDiscoveryExploreClient.
          // Also binding to instance is fine here as the programId is guaranteed to not change throughout the
          // lifecycle of this program runnable
          bind(ProgramId.class).toInstance(programId);

          // bind explore client to ProgramDiscoveryExploreClient which is aware of the programId
          bind(ExploreClient.class).to(ProgramDiscoveryExploreClient.class).in(Scopes.SINGLETON);

          // Bind the ArtifactManager implementation
          install(new FactoryModuleBuilder()
                    .implement(ArtifactManager.class, RemoteArtifactManager.class)
                    .build(ArtifactManagerFactory.class));

          // Bind the PluginFinder implementation
          bind(PluginFinder.class).to(RemotePluginFinder.class);
        }
      }
    ));
  }

  private Module createStreamFactoryModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder().implement(StreamWriter.class, DefaultStreamWriter.class)
                  .build(StreamWriterFactory.class));
        expose(StreamWriterFactory.class);
      }
    };
  }

  private static String generateClientId(ProgramRunId programRunId, String instanceId) {
    return String.format("%s.%s.%s", programRunId.getParent(), programRunId.getRun(), instanceId);
  }
}
