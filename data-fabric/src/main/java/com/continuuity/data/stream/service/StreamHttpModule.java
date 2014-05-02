/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream.service;

import com.continuuity.common.conf.Constants;
import com.continuuity.data.stream.StreamHandler;
import com.continuuity.data.stream.StreamPingHandler;
import com.continuuity.http.HttpHandler;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

/**
 *
 */
public class StreamHttpModule extends PrivateModule {

  @Override
  protected void configure() {
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class,
                                                                      Names.named(Constants.Service.STREAM_HANDLER));
    handlerBinder.addBinding().to(StreamHandler.class);
    handlerBinder.addBinding().to(StreamPingHandler.class);
    bind(StreamHttpService.class).in(Scopes.SINGLETON);
    expose(StreamHttpService.class);
  }
}
