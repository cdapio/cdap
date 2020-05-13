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

package io.cdap.cdap.app.guice;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.CommonHandlers;
import io.cdap.cdap.internal.app.runtime.monitor.DirectRuntimeRequestValidator;
import io.cdap.cdap.internal.app.runtime.monitor.LogAppenderLogProcessor;
import io.cdap.cdap.internal.app.runtime.monitor.RemoteExecutionLogProcessor;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeHandler;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeRequestValidator;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeServer;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeServiceRoutingHandler;
import io.cdap.cdap.logging.gateway.handlers.ProgramRunRecordFetcher;
import io.cdap.http.HttpHandler;

/**
 * A Guice module for exposing {@link RuntimeServer} for runtime monitoring.
 */
public class RuntimeServerModule extends PrivateModule {

  @Override
  protected void configure() {
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class,
                                                                      Names.named(Constants.Service.RUNTIME));
    handlerBinder.addBinding().to(RuntimeHandler.class);
    handlerBinder.addBinding().to(RuntimeServiceRoutingHandler.class);
    CommonHandlers.add(handlerBinder);

    bindRequestValidator();
    bindLogProcessor();
    bind(ProgramRunRecordFetcher.class).toProvider(ProgramRunRecordFetcherProvider.class);

    bind(RuntimeServer.class).in(Scopes.SINGLETON);
    expose(RuntimeServer.class);
  }

  @VisibleForTesting
  protected void bindRequestValidator() {
    bind(RuntimeRequestValidator.class).to(DirectRuntimeRequestValidator.class).in(Scopes.SINGLETON);
  }

  @VisibleForTesting
  protected void bindLogProcessor() {
    bind(RemoteExecutionLogProcessor.class).to(LogAppenderLogProcessor.class).in(Scopes.SINGLETON);
  }

  /**
   * Provider for {@link ProgramRunRecordFetcher}. Implementation returned is based on CDAP configuration.
   */
  private static final class ProgramRunRecordFetcherProvider implements Provider<ProgramRunRecordFetcher> {

    private final Injector injector;
    private final Class<? extends ProgramRunRecordFetcher> fetcherClass;

    @Inject
    ProgramRunRecordFetcherProvider(Injector injector, CConfiguration cConf) {
      this.injector = injector;
      this.fetcherClass = cConf.getClass(Constants.RuntimeMonitor.RUN_RECORD_FETCHER_CLASS, null,
                                         ProgramRunRecordFetcher.class);
      if (fetcherClass == null) {
        throw new IllegalStateException("Missing configuration " + Constants.RuntimeMonitor.RUN_RECORD_FETCHER_CLASS);
      }
    }

    @Override
    public ProgramRunRecordFetcher get() {
      return injector.getInstance(fetcherClass);
    }
  }
}
