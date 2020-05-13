/*
 * Copyright © 2019-2020 Cask Data, Inc.
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

package io.cdap.cdap.logging.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.runtime.RuntimeModule;
import io.cdap.cdap.gateway.handlers.CommonHandlers;
import io.cdap.cdap.logging.gateway.handlers.LocalProgramRunRecordFetcher;
import io.cdap.cdap.logging.gateway.handlers.LogHttpHandler;
import io.cdap.cdap.logging.gateway.handlers.ProgramRunRecordFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordFetcher;
import io.cdap.cdap.logging.service.LogQueryService;
import io.cdap.http.HttpHandler;

/**
 * A Guice module to provide bindings for the HTTP service for querying logs.
 */
public class LogQueryRuntimeModule extends RuntimeModule {

  private static void bindHandlers(Binder binder) {
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder, HttpHandler.class,
                                                                      Names.named(Constants.Service.LOG_QUERY));
    handlerBinder.addBinding().to(LogHttpHandler.class);
    CommonHandlers.add(handlerBinder);
  }

  private Module getLocalModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bindHandlers(binder());
        bind(ProgramRunRecordFetcher.class).to(LocalProgramRunRecordFetcher.class);
        bind(LogQueryService.class).in(Scopes.SINGLETON);
        expose(LogQueryService.class);
      }
    };
  }

  private Module getRemoteModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bindHandlers(binder());
        bind(ProgramRunRecordFetcher.class).to(RemoteProgramRunRecordFetcher.class);
        bind(LogQueryService.class).in(Scopes.SINGLETON);
        expose(LogQueryService.class);
      }
    };
  }

  @Override
  public Module getInMemoryModules() {
    return getLocalModule();
  }

  @Override
  public Module getStandaloneModules() {
    return getLocalModule();
  }

  @Override
  public Module getDistributedModules() {
    return getRemoteModule();
  }
}
