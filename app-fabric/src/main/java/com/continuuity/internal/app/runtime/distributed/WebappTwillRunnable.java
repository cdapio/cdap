/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.gateway.handlers.AppFabricGatewayModule;
import com.continuuity.gateway.handlers.GatewayCommonHandlerModule;
import com.continuuity.internal.app.runtime.webapp.ExplodeJarHttpHandler;
import com.continuuity.internal.app.runtime.webapp.JarHttpHandler;
import com.continuuity.internal.app.runtime.webapp.WebappHttpHandlerFactory;
import com.continuuity.internal.app.runtime.webapp.WebappProgramRunner;
import com.continuuity.metrics.guice.MetricsHandlerModule;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.util.Modules;
import org.apache.twill.api.TwillContext;

/**
 * Twill runnable wrapper for webapp.
 */
final class WebappTwillRunnable extends AbstractProgramTwillRunnable<WebappProgramRunner> {

  WebappTwillRunnable(String name, String hConfName, String cConfName) {
    super(name, hConfName, cConfName);
  }

  @Override
  protected Class<WebappProgramRunner> getProgramClass() {
    return WebappProgramRunner.class;
  }

  @Override
  protected Module createModule(TwillContext context) {
    return Modules.combine(super.createModule(context),
                           new DiscoveryRuntimeModule().getDistributedModules(),
                           new AuthModule(),
                           new GatewayCommonHandlerModule(),
                           new AppFabricGatewayModule(),
                           new MetricsHandlerModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               // Create webapp http handler factory.
                               install(new FactoryModuleBuilder()
                                         .implement(JarHttpHandler.class, ExplodeJarHttpHandler.class)
                                         .build(WebappHttpHandlerFactory.class));
                             }
                           });
  }
}
