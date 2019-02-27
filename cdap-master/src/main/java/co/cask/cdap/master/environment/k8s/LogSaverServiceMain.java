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

package co.cask.cdap.master.environment.k8s;

import co.cask.cdap.api.logging.AppenderContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.LocalLocationModule;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.framework.distributed.DistributedAppenderContext;
import co.cask.cdap.logging.guice.LogQueryServerModule;
import co.cask.cdap.logging.logbuffer.LogBufferService;
import co.cask.cdap.logging.read.FileLogReader;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.logging.service.LogQueryService;
import co.cask.cdap.logging.service.LogSaverStatusService;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.impersonation.CurrentUGIProvider;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.http.HttpHandler;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Main class for running {@link LogBufferService} and {@link LogQueryService} in Kubernetes.
 */
public class LogSaverServiceMain extends AbstractServiceMain {

  /**
   * Main entry point.
   */
  public static void main(String[] args) throws Exception {
    main(LogSaverServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules() {
    return Arrays.asList(
      new NamespaceQueryAdminModule(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new AuthenticationContextModules().getMasterModule(),
      new MessagingClientModule(),
      getDataFabricModule(),
      // Always use local table implementations, which use LevelDB.
      // In K8s, there won't be HBase and the cdap-site should be set to use SQL store for StructuredTable.
      new SystemDatasetRuntimeModule().getStandaloneModules(),
      new DataSetsModules().getStandaloneModules(),
      new LocalLocationModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // Current impersonation is not supported. Its is needed by FileLogReader
          bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);
          bind(LogReader.class).to(FileLogReader.class);
          bind(LogAppender.class).to(NoopAppender.class).in(Scopes.SINGLETON);
        }
      },
      // log handler is co-located with log saver
      new LogQueryServerModule(),
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(Integer.class).annotatedWith(Names.named(Constants.LogSaver.LOG_SAVER_INSTANCE_ID))
            .toInstance(0);
          bind(Integer.class).annotatedWith(Names.named(Constants.LogSaver.LOG_SAVER_INSTANCE_COUNT))
            .toInstance(1);
          bind(AppenderContext.class).to(DistributedAppenderContext.class);

          // Bind the status service
          Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder
            (binder(), HttpHandler.class, Names.named(Constants.LogSaver.LOG_SAVER_STATUS_HANDLER));
          CommonHandlers.add(handlerBinder);
          bind(LogSaverStatusService.class).in(Scopes.SINGLETON);
          expose(LogSaverStatusService.class);

          bind(LogBufferService.class).in(Scopes.SINGLETON);
          expose(LogBufferService.class);
        }
      }
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources) {
    // log saver
    services.add(injector.getInstance(LogBufferService.class));
    // log handler
    services.add(injector.getInstance(LogQueryService.class));
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext() {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.LOGSAVER);
  }

  /**
   * A {@link LogAppender} that is Noop.
   */
  private static final class NoopAppender extends LogAppender {

    @Override
    protected void appendEvent(LogMessage logMessage) {
      // no-op
    }
  }
}
