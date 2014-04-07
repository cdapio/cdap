package com.continuuity.gateway.handlers;

import com.continuuity.gateway.handlers.dataset.ClearFabricHandler;
import com.continuuity.gateway.handlers.dataset.DatasetHandler;
import com.continuuity.gateway.handlers.dataset.TableHandler;
import com.continuuity.gateway.handlers.procedure.ProcedureHandler;
import com.continuuity.gateway.handlers.stream.StreamHandler;
import com.continuuity.http.HttpHandler;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

/**
 * Guice module for gateway handlers defined in app fabric.
 */
public class AppFabricGatewayModule extends AbstractModule {
  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(StreamHandler.class).in(Scopes.SINGLETON);
        bind(AppFabricServiceHandler.class).in(Scopes.SINGLETON);
        bind(ProcedureHandler.class).in(Scopes.SINGLETON);
        bind(WorkflowHandler.class).in(Scopes.SINGLETON);
        bind(TableHandler.class).in(Scopes.SINGLETON);
        bind(DatasetHandler.class).in(Scopes.SINGLETON);
        bind(ClearFabricHandler.class).in(Scopes.SINGLETON);

        expose(StreamHandler.class);
        expose(AppFabricServiceHandler.class);
        expose(ProcedureHandler.class);
        expose(WorkflowHandler.class);
        expose(TableHandler.class);
        expose(DatasetHandler.class);
        expose(ClearFabricHandler.class);

      }
    });

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class);
    handlerBinder.addBinding().to(StreamHandler.class);
    handlerBinder.addBinding().to(AppFabricServiceHandler.class);
    handlerBinder.addBinding().to(ProcedureHandler.class);
    handlerBinder.addBinding().to(WorkflowHandler.class);
    handlerBinder.addBinding().to(TableHandler.class);
    handlerBinder.addBinding().to(DatasetHandler.class);
    handlerBinder.addBinding().to(ClearFabricHandler.class);
  }
}
