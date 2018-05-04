/*
 * Copyright © 2018 Cask Data, Inc.
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

import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.NoLookupNamespacedLocationFactory;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.InMemoryStreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.metadata.writer.MessagingLineageWriter;
import co.cask.cdap.data2.registry.MessagingUsageWriter;
import co.cask.cdap.data2.registry.UsageWriter;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.internal.app.program.MessagingProgramStateWriter;
import co.cask.cdap.internal.app.runtime.workflow.MessagingWorkflowStateWriter;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.security.impersonation.CurrentUGIProvider;
import co.cask.cdap.security.impersonation.NoOpOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.UGIProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ServiceAnnouncer;

import java.util.ArrayList;
import java.util.Collections;
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
  private final String instanceId;
  private final ClusterMode clusterMode;
  @Nullable
  private final String principal;
  @Nullable
  private final ServiceAnnouncer serviceAnnouncer;

  private DistributedProgramContainerModule(CConfiguration cConf, Configuration hConf, ProgramRunId programRunId,
                                            String instanceId, ClusterMode clusterMode,
                                            @Nullable String principal, @Nullable ServiceAnnouncer serviceAnnouncer) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.programRunId = programRunId;
    this.instanceId = instanceId;
    this.clusterMode = clusterMode;
    this.principal = principal;
    this.serviceAnnouncer = serviceAnnouncer;
  }

  @Override
  protected void configure() {
    List<Module> modules = getCoreModules(programRunId.getParent());

    AuthenticationContextModules authModules = new AuthenticationContextModules();
    modules.add(principal == null
                  ? authModules.getProgramContainerModule()
                  : authModules.getProgramContainerModule(principal));

    install(Modules.override(modules).with(new AbstractModule() {
      @Override
      protected void configure() {
        // Overrides the LineageWriter, UsageWriter to write to TMS instead
        bind(LineageWriter.class).to(MessagingLineageWriter.class);
        bind(UsageWriter.class).to(MessagingUsageWriter.class);
      }
    }));
  }

  private List<Module> getCoreModules(final ProgramId programId) {
    List<Module> modules = new ArrayList<>();

    modules.add(new ConfigModule(cConf, hConf));
    modules.add(new IOModule());
    modules.add(new ZKClientModule());
    modules.add(new MetricsClientRuntimeModule().getDistributedModules());
    modules.add(new MessagingClientModule());
    modules.add(new DiscoveryRuntimeModule().getDistributedModules());
    modules.add(new AuditModule().getDistributedModules());
    modules.add(new AuthorizationEnforcementModule().getDistributedModules());
    modules.add(new SecureStoreModules().getDistributedModules());
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
    modules.add(new LocationRuntimeModule().getDistributedModules());
    modules.add(new KafkaClientModule());
    modules.add(new LoggingModules().getDistributedModules());
    modules.add(new DataFabricModules(generateClientId(programRunId, instanceId)).getDistributedModules());
    modules.add(new DataSetsModules().getDistributedModules());
    modules.add(new NamespaceClientRuntimeModule().getDistributedModules());
    modules.add(new DistributedProgramStreamModule());
  }

  private void addIsolatedModules(List<Module> modules) {
    modules.add(new DataSetsModules().getStandaloneModules());
    modules.add(new DataSetServiceModules().getStandaloneModules());
    // Use the in memory transaction module, as we don't need to recover from tx problem as the tx is only local
    // to this run.
    modules.add(new DataFabricModules().getInMemoryModules());

    // In isolated mode, ignore the namespace mapping
    modules.add(Modules.override(new LocationRuntimeModule().getDistributedModules()).with(new AbstractModule() {
      @Override
      protected void configure() {
        bind(NamespacedLocationFactory.class).to(NoLookupNamespacedLocationFactory.class);
      }
    }));

    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        // TODO (CDAP-13380): Use a LogAppender defined by the runtime provider
        bind(LogAppender.class).toInstance(new LogAppender() {
          @Override
          protected void appendEvent(LogMessage logMessage) {
            // no-op
          }
        });

        // Bind to unsupported/no-op class implementations for features that are not supported in isolated cluster mode
        bind(StreamAdmin.class).to(UnsupportedStreamAdmin.class);
        bind(StreamCoordinatorClient.class).to(InMemoryStreamCoordinatorClient.class);
        bind(ExploreClient.class).to(UnsupportedExploreClient.class);
        bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);

        // This is just for Dataset Service to check if a namespace exists
        bind(NamespaceQueryAdmin.class).toInstance(new NamespaceQueryAdmin() {
          @Override
          public List<NamespaceMeta> list() throws Exception {
            return Collections.singletonList(get(programRunId.getNamespaceId()));
          }

          @Override
          public NamespaceMeta get(NamespaceId namespaceId) throws Exception {
            return new NamespaceMeta.Builder().setName(namespaceId).build();
          }

          @Override
          public boolean exists(NamespaceId namespaceId) throws Exception {
            return programRunId.getNamespaceId().equals(namespaceId);
          }
        });
      }
    });
  }

  private static String generateClientId(ProgramRunId programRunId, String instanceId) {
    return String.format("%s.%s.%s", programRunId.getParent(), programRunId.getRun(), instanceId);
  }

  /**
   * Creates a {@link Builder} for building {@link DistributedProgramContainerModule}.
   */
  public static Builder builder(CConfiguration cConf, Configuration hConf,
                                ProgramRunId programRunId, String instanceId) {
    return new Builder(cConf, hConf, programRunId, instanceId);
  }

  /**
   * Builder for the {@link DistributedProgramContainerModule}. By default it builds the module used for
   * {@link ClusterMode#ON_PREMISE}, unless changed via the {@link #setClusterMode(ClusterMode)} method.
   */
  public static final class Builder {

    private final CConfiguration cConf;
    private final Configuration hConf;
    private final ProgramRunId programRunId;
    private final String instanceId;
    private String principal;
    private ServiceAnnouncer serviceAnnouncer;
    private ClusterMode clusterMode = ClusterMode.ON_PREMISE;

    private Builder(CConfiguration cConf, Configuration hConf, ProgramRunId programRunId, String instanceId) {
      this.cConf = cConf;
      this.hConf = hConf;
      this.programRunId = programRunId;
      this.instanceId = instanceId;
    }

    public Builder setPrincipal(String principal) {
      this.principal = principal;
      return this;
    }

    public Builder setServiceAnnouncer(ServiceAnnouncer serviceAnnouncer) {
      this.serviceAnnouncer = serviceAnnouncer;
      return this;
    }

    public Builder setClusterMode(ClusterMode clusterMode) {
      this.clusterMode = clusterMode;
      return this;
    }

    public DistributedProgramContainerModule build() {
      return new DistributedProgramContainerModule(cConf, hConf, programRunId, instanceId,
                                                   clusterMode, principal, serviceAnnouncer);
    }
  }
}
