/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.guice;

import co.cask.cdap.api.logging.AppenderContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.cdap.logging.framework.distributed.DistributedAppenderContext;
import co.cask.cdap.logging.framework.distributed.DistributedLogFramework;
import co.cask.cdap.logging.service.LogSaverStatusService;
import co.cask.http.HttpHandler;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import org.apache.twill.api.TwillContext;

/**
 * Guice module for the distributed log framework.
 */
public class DistributedLogFrameworkModule extends PrivateModule {

  private final TwillContext twillContext;

  public DistributedLogFrameworkModule(TwillContext twillContext) {
    this.twillContext = twillContext;
  }

  @Override
  protected void configure() {
    bind(TwillContext.class).toInstance(twillContext);
    bind(AppenderContext.class).to(DistributedAppenderContext.class);

    // Bind the status service
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder
      (binder(), HttpHandler.class, Names.named(Constants.LogSaver.LOG_SAVER_STATUS_HANDLER));
    CommonHandlers.add(handlerBinder);
    bind(LogSaverStatusService.class).in(Scopes.SINGLETON);
    expose(LogSaverStatusService.class);

    bind(DistributedLogFramework.class).in(Scopes.SINGLETON);
    expose(DistributedLogFramework.class);
  }
}
