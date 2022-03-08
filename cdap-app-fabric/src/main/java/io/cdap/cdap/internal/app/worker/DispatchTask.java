/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.app.deploy.DispatchResponse;
import io.cdap.cdap.app.guice.ImpersonatedTwillRunnerService;
import io.cdap.cdap.app.guice.RemoteExecutionProgramRunnerModule;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.deploy.InMemoryDispatcher;
import io.cdap.cdap.internal.app.deploy.pipeline.AppLaunchInfo;
import io.cdap.cdap.internal.app.runtime.artifact.ApplicationClassCodec;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RequirementsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.internal.provision.DefaultProvisionerConfigProvider;
import io.cdap.cdap.internal.provision.ProvisionerConfigProvider;
import io.cdap.cdap.internal.provision.ProvisionerExtensionLoader;
import io.cdap.cdap.internal.provision.ProvisionerModule;
import io.cdap.cdap.internal.provision.ProvisionerProvider;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metrics.guice.DistributedMetricsClientModule;
import io.cdap.cdap.security.auth.KeyManager;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.guice.CoreSecurityModule;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.guice.DistributedCoreSecurityModule;
import io.cdap.cdap.security.guice.ExternalAuthenticationModule;
import io.cdap.cdap.security.guice.FileBasedCoreSecurityModule;
import io.cdap.cdap.security.guice.SecureStoreClientModule;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class DispatchTask implements RunnableTask {

  private static final Logger LOG = LoggerFactory.getLogger(DispatchTask.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
          new GsonBuilder())
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(ApplicationClass.class, new ApplicationClassCodec())
      .registerTypeAdapter(Requirements.class, new RequirementsCodec())
      .registerTypeAdapter(RunId.class, new RunIds.RunIdCodec())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
      .create();

  private final CConfiguration cConf;
  private final SConfiguration sConf;
  private final KeyManager keyManager;
  private final ProvisionerProvider provisionerProvider;
  private final TwillRunnerService twillRunnerService;

  @Inject
  DispatchTask(CConfiguration cConf, SConfiguration sConf, KeyManager keyManager,
      ProvisionerProvider provisionerProvider, TwillRunnerService twillRunnerService) {
    this.cConf = cConf;
    this.sConf = sConf;
    this.keyManager = keyManager;
    this.provisionerProvider = provisionerProvider;
    this.twillRunnerService = twillRunnerService;
  }

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    try {
      LOG.debug("KeyManager reference for DispatchTask: {}", keyManager.toString());
      AppLaunchInfo appLaunchInfo = GSON.fromJson(context.getParam(), AppLaunchInfo.class);
      Injector injector = Guice.createInjector(
          new ConfigModule(cConf, sConf),
          new DFSLocationModule(),
          new ConfiguratorTaskModule(),
          new DispatchTaskModule(),
          RemoteAuthenticatorModules.getDefaultModule(),
          new AuthenticationContextModules().getMasterWorkerModule(),
          new ExternalAuthenticationModule(),
          // new RemoteExecutionProgramRunnerModule(),
          new SecureStoreClientModule(),
          new IOModule(),
          new ProvisionerModule() {
            @Override
            protected void configure() {
              bind(ProvisioningService.class).in(Scopes.SINGLETON);
              bind(ProvisionerProvider.class).toInstance(provisionerProvider);
              bind(ProvisionerConfigProvider.class).to(DefaultProvisionerConfigProvider.class);
            }
          },
          // new DistributedMetricsClientModule(),
          new MessagingClientModule(),
          new StorageModule(),
          new ZKClientModule(),
          new CoreSecurityModule() {
            @Override
            protected void bindKeyManager(Binder binder) {
              bind(KeyManager.class).toInstance(keyManager);
            }
          },
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(TwillRunnerService.class).annotatedWith(Constants.AppFabric.ProgramRunner.class)
                  .toInstance(twillRunnerService);
            }
          }
          // new FileBasedCoreSecurityModule()
          // CoreSecurityRuntimeModule.getDistributedModule(cConf)
      );
      DispatchTaskRunner taskRunner = injector.getInstance(DispatchTaskRunner.class);
      DispatchResponse response = taskRunner.dispatch(appLaunchInfo);
      if (response.getExitCode() == 0 && response.isSuccessfulLaunch()) {
        context.setTerminateOnComplete(true);
      }
      String result = GSON.toJson(response);
      context.writeResult(result.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      LOG.error("Exception: ", e);
    }
  }

  private static class DispatchTaskRunner {

    private final CConfiguration cConf;
    private final ProgramRunnerFactory programRunnerFactory;
    private final ConfiguratorFactory configuratorFactory;
    private final Impersonator impersonator;
    private final ArtifactRepository artifactRepository;
    private ProgramRunnerFactory remoteProgramRunnerFactory;
    private InetAddress host;

    @Inject
    DispatchTaskRunner(CConfiguration cConf,
        ProgramRunnerFactory programRunnerFactory,
        ConfiguratorFactory configuratorFactory,
        Impersonator impersonator,
        ArtifactRepository artifactRepository,
        TwillRunnerService twillRunnerService) {
      this.cConf = cConf;
      this.programRunnerFactory = programRunnerFactory;
      this.configuratorFactory = configuratorFactory;
      this.impersonator = impersonator;
      this.artifactRepository = artifactRepository;
      LOG.debug("TwillRunnerService in DispatchTask: {}", twillRunnerService);
      if (twillRunnerService instanceof ImpersonatedTwillRunnerService) {
        LOG.debug("TwillRunnerService in DispatchTask internal: {}",
            ((ImpersonatedTwillRunnerService) twillRunnerService).delegate);
      }
    }

    /**
     * Optional guice injection for the {@link ProgramRunnerFactory} used for remote execution. It is
     * optional because in unit-test we don't have need for that.
     */
    @Inject(optional = true)
    void setRemoteProgramRunnerFactory(
        @Constants.AppFabric.RemoteExecution ProgramRunnerFactory runnerFactory) {
      this.remoteProgramRunnerFactory = runnerFactory;
    }

    @Inject(optional = true)
    void setHostname(@Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS) InetAddress host) {
      this.host = host;
    }

    public DispatchResponse dispatch(AppLaunchInfo appLaunchInfo) throws Exception {
      InMemoryDispatcher dispatcher = new InMemoryDispatcher(cConf, programRunnerFactory,
          configuratorFactory, impersonator, artifactRepository, appLaunchInfo);
      dispatcher.setRemoteProgramRunnerFactory(remoteProgramRunnerFactory);
      dispatcher.setHostname(host);
      try {
        return dispatcher.dispatch().get(120, TimeUnit.SECONDS);
      } catch (Exception e) {
        // We don't need the ExecutionException being reported back to the RemoteTaskExecutor, hence only
        // propagating the actual cause.
        Throwables.propagateIfPossible(e.getCause(), Exception.class);
        throw Throwables.propagate(e.getCause());
      }
    }
  }
}
