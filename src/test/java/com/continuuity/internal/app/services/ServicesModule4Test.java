/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.app.services.RuntimeService;
import com.google.inject.AbstractModule;

public class ServicesModule4Test extends AbstractModule {
  @Override
  protected void configure() {
    bind(RuntimeService.Iface.class).to(RuntimeServiceImpl.class);
  }
}
