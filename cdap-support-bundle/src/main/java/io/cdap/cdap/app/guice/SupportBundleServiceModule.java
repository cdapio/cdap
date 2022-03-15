/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.app.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.SupportBundle;
import io.cdap.cdap.common.guice.HealthCheckModule;
import io.cdap.cdap.common.service.HealthCheckService;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.gateway.handlers.CommonHandlers;
import io.cdap.cdap.gateway.handlers.HealthCheckHttpHandler;
import io.cdap.cdap.handlers.SupportBundleHttpHandler;
import io.cdap.cdap.internal.app.services.SupportBundleInternalService;
import io.cdap.cdap.task.factory.SupportBundleK8sHealthCheckTaskFactory;
import io.cdap.cdap.task.factory.SupportBundlePipelineInfoTaskFactory;
import io.cdap.cdap.task.factory.SupportBundleSystemLogTaskFactory;
import io.cdap.cdap.task.factory.SupportBundleTaskFactory;
import io.cdap.http.HttpHandler;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * SupportBundle Service Guice Module.
 */
public final class SupportBundleServiceModule extends AbstractModule {

  @Override
  protected void configure() {
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(
      binder(), HttpHandler.class, Names.named(Constants.SupportBundle.HANDLERS_NAME));
    CommonHandlers.add(handlerBinder);
    handlerBinder.addBinding().to(SupportBundleHttpHandler.class);
    handlerBinder.addBinding().to(HealthCheckHttpHandler.class);
    bind(HealthCheckService.class).in(Scopes.SINGLETON);
    bind(SupportBundleInternalService.class).in(Scopes.SINGLETON);
    Multibinder<SupportBundleTaskFactory> supportBundleTaskFactoryMultibinder = Multibinder.newSetBinder(
      binder(), SupportBundleTaskFactory.class, Names.named(SupportBundle.TASK_FACTORY));
    supportBundleTaskFactoryMultibinder.addBinding().to(SupportBundlePipelineInfoTaskFactory.class);
    supportBundleTaskFactoryMultibinder.addBinding().to(SupportBundleSystemLogTaskFactory.class);
    supportBundleTaskFactoryMultibinder.addBinding().to(SupportBundleK8sHealthCheckTaskFactory.class);
    install(new HealthCheckModule());
  }

  @Provides
  @Named(Constants.SupportBundle.SERVICE_BIND_ADDRESS)
  @SuppressWarnings("unused")
  public InetAddress providesHostname(CConfiguration cConf) {
    String address = cConf.get(SupportBundle.SERVICE_BIND_ADDRESS);
    return Networks.resolve(address, new InetSocketAddress("localhost", 0).getAddress());
  }
}
