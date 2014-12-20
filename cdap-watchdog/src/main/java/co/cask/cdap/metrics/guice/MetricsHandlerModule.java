/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.metrics.guice;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.PingHandler;
import co.cask.cdap.logging.gateway.handlers.LogHandler;
import co.cask.cdap.logging.gateway.handlers.LogHandlerV2;
import co.cask.cdap.metrics.data.DefaultMetricsTableFactory;
import co.cask.cdap.metrics.data.MetricsTableFactory;
import co.cask.cdap.metrics.query.BatchMetricsHandler;
import co.cask.cdap.metrics.query.DeleteMetricsHandler;
import co.cask.cdap.metrics.query.MetricsDiscoveryHandler;
import co.cask.cdap.metrics.query.MetricsQueryHandler;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.http.HttpHandler;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

/**
 * Metrics http handlers.
 */
public class MetricsHandlerModule extends PrivateModule {
  @Override
  protected void configure() {
    bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);

    bind(MetricsQueryService.class).in(Scopes.SINGLETON);
    expose(MetricsQueryService.class);

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class,
                                                                      Names.named(Constants.Service.METRICS));
    handlerBinder.addBinding().to(BatchMetricsHandler.class);
    handlerBinder.addBinding().to(DeleteMetricsHandler.class);
    handlerBinder.addBinding().to(MetricsDiscoveryHandler.class);
    handlerBinder.addBinding().to(MetricsQueryHandler.class);
    handlerBinder.addBinding().to(LogHandlerV2.class);
    handlerBinder.addBinding().to(LogHandler.class);
    handlerBinder.addBinding().to(PingHandler.class);
  }
}
