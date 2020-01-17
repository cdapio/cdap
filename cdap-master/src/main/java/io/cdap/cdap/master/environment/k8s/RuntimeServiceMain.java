/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import io.cdap.cdap.internal.app.runtime.RuntimeService;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.internal.app.runtime.RuntimeService;
import io.cdap.cdap.internal.app.runtime.monitor.LogAppenderLogProcessor;
import io.cdap.cdap.internal.app.runtime.monitor.RemoteExecutionLogProcessor;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.runtime.RuntimeServiceModule;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

/**
 * Service that receives runtime metadata.
 */
public class RuntimeServiceMain extends AbstractServiceMain<EnvironmentOptions> {
    /**
     * Main entry point
     */
    public static void main(String[] args) throws Exception {
        main(RuntimeServiceMain.class, args);
    }

    @Override
    protected List<Module> getServiceModules(MasterEnvironment masterEnv, EnvironmentOptions options) {
        return Arrays.asList(
//                new ProgramRunnerRuntimeModule().getDistributedModules(),
                new RuntimeServiceModule(),
                new RemoteLogAppenderModule(),
////                new ProvisionerModule(),
                new MessagingClientModule(),
//                new RuntimeServiceModule(),
//                new SecureStoreServerModule(),
////                new NamespaceAdminModule().getDistributedModules(),
//                new AuthenticationContextModules().getMasterModule(),
//                new DFSLocationModule(),
////                new DataSetsModules().getDistributedModules(),
//                new AuthorizationEnforcementModule().getDistributedModules(),
//                new AuthenticationContextModules().getMasterModule(),
////                new MetricsStoreModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(RemoteExecutionLogProcessor.class).to(LogAppenderLogProcessor.class).in(Scopes.SINGLETON);
//                        bind(TwillRunnerService.class).toProvider(
//                                new SupplierProviderBridge<>(masterEnv.getTwillRunnerSupplier())).in(Scopes.SINGLETON);
//                        bind(TwillRunner.class).to(TwillRunnerService.class);
//                        // Current impersonation is not supported
//                        bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);
//                        bind(ArtifactRepository.class)
//                                .annotatedWith(Names.named(NOAUTH_ARTIFACT_REPO))
//                                .to(DefaultArtifactRepository.class)
//                                .in(Scopes.SINGLETON);
//                        bind(MetadataServiceClient.class).to(DefaultMetadataServiceClient.class);
//                        bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);
                    }
                }
        );
    }

    @Override
    protected void addServices(Injector injector, List<? super Service> services,
                               List<? super AutoCloseable> closeableResources,
                               MasterEnvironment masterEnv, MasterEnvironmentContext masterEnvContext,
                               EnvironmentOptions options) {
        services.add(injector.getInstance(RuntimeService.class));
    }

    @Nullable
    @Override
    protected LoggingContext getLoggingContext(EnvironmentOptions options) {
        return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                Constants.Logging.COMPONENT_NAME,
                Constants.Service.RUNTIME_HTTP);
    }
}
