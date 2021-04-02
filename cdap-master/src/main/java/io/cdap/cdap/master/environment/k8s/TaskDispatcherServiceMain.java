package io.cdap.cdap.master.environment.k8s;


import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.gateway.handlers.CommonHandlers;
import io.cdap.cdap.internal.app.dispatcher.TaskDispatcherHttpHandlerInternal;
import io.cdap.cdap.internal.app.dispatcher.TaskDispatcherServer;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.HttpHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;


public class TaskDispatcherServiceMain extends AbstractServiceMain<EnvironmentOptions> {
  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(TaskDispatcherServiceMain.class, args);
  }

  @Override
  protected CConfiguration updateCConf(CConfiguration cConf) {
    return cConf;
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv,
                                           EnvironmentOptions options, CConfiguration cConf) {
    List<Module> modules = new ArrayList<>(Arrays.asList(
      new DFSLocationModule(),
      new MessagingClientModule(),
      new PrivateModule() {
        @Override
        protected void configure() {
          Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(
            binder(), HttpHandler.class, Names.named(Constants.TaskDispatcher.HANDLER_NAME));
          handlerBinder.addBinding().to(TaskDispatcherHttpHandlerInternal.class);
          CommonHandlers.add(handlerBinder);

          bind(TaskDispatcherServer.class).in(Scopes.SINGLETON);
          expose(TaskDispatcherServer.class);
        }
      }
    ));
    return modules;
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources,
                             MasterEnvironment masterEnv,
                             MasterEnvironmentContext masterEnvContext,
                             EnvironmentOptions options) {
    services.add(injector.getInstance(TaskDispatcherServer.class));
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.TASK_DISPATCHER);
  }
}
