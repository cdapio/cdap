/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream.service;

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

/**
 *
 */
public class StreamHttpModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(StreamHttpService.class).in(Scopes.SINGLETON);
    expose(StreamHttpService.class);
  }
}
