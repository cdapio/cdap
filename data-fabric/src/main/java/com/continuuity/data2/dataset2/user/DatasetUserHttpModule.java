package com.continuuity.data2.dataset2.user;

import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset2.manager.inmemory.DefaultDatasetDefinitionRegistry;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

/**
 * Module containing {@link DatasetUserService}.
 */
public class DatasetUserHttpModule extends PrivateModule {

  @Override
  protected void configure() {
    Named datasetUserName = Names.named(Constants.Service.DATASET_USER);
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class, datasetUserName);
    handlerBinder.addBinding().to(DatasetAdminHTTPHandler.class);
    handlerBinder.addBinding().to(PingHandler.class);

    bind(DatasetDefinitionRegistry.class).to(DefaultDatasetDefinitionRegistry.class);
    bind(DatasetUserService.class).in(Scopes.SINGLETON);
    expose(DatasetUserService.class);
  }
}
