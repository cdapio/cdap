/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.k8s;

import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.guice.UnsupportedExploreClient;
import io.cdap.cdap.app.preview.PreviewRunnerManager;
import io.cdap.cdap.app.preview.PreviewRunnerManagerModule;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.internal.app.preview.PreviewRequestPollerInfoProvider;
import io.cdap.cdap.master.environment.k8s.AbstractServiceMain;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metadata.MetadataServiceModule;
import io.cdap.cdap.metrics.guice.MetricsStoreModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.SecureStoreClientModule;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * The main class to run the preview runner. Preview runner will run in its own pod.
 */
public class PreviewRunnerMain extends AbstractServiceMain<PreviewRunnerOptions> {
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(PreviewRunnerMain.class);
  private PreviewRequestPollerInfo previewRequestPollerInfo;

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(PreviewRunnerMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv, PreviewRunnerOptions options) {
    return Arrays.asList(
      new PreviewRunnerManagerModule().getDistributedModules(),
      new DataSetServiceModules().getStandaloneModules(),
      new DataSetsModules().getStandaloneModules(),
      new AppFabricServiceRuntimeModule().getStandaloneModules(),
      new ProgramRunnerRuntimeModule().getStandaloneModules(),
      new MetricsStoreModule(),
      new MessagingClientModule(),
      new AuditModule(),
      new SecureStoreClientModule(),
      new MetadataReaderWriterModules().getStandaloneModules(),
      getDataFabricModule(),
      new DFSLocationModule(),
      new MetadataServiceModule(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(ExploreClient.class).to(UnsupportedExploreClient.class);
          bind(PreviewRequestPollerInfoProvider.class).toInstance(
            () -> Bytes.toBytes(GSON.toJson(previewRequestPollerInfo)));
        }
      }
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources, MasterEnvironment masterEnv,
                             MasterEnvironmentContext masterEnvContext, PreviewRunnerOptions options) {
    Service previewRunnerManager = (Service) injector.getInstance(PreviewRunnerManager.class);
    previewRunnerManager.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        terminate();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        terminate();
      }

      private void terminate() {
        System.exit(0);
      }
    }, command -> {
      Thread thread = new Thread(command, "PreviewRunnerMainTerminator");
      thread.setDaemon(true);
      thread.start();
    });
    services.add(previewRunnerManager);
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(PreviewRunnerOptions options) {
    String instanceName = readFile(options.getInstanceNameFilePath());
    String instanceUid = readFile(options.getInstanceUidFilePath());
    LOG.info("Instance name: {}, Instance UID: {}", instanceName, instanceUid);
    previewRequestPollerInfo = new PreviewRequestPollerInfo(instanceName, instanceUid);
    LOG.info("Instance id: {}, Instance UID: {}", previewRequestPollerInfo.getInstanceId(),
             previewRequestPollerInfo.getInstanceUid());
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.PREVIEW_HTTP);
  }

  private static String readFile(String fileName) {
    try {
      return Bytes.toString(Files.readAllBytes(Paths.get(fileName)));
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Unable to read file %s", fileName), e);
    }
  }
}
