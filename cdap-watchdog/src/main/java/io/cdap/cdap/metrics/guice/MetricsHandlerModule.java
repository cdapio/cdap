/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.metrics.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.CommonHandlers;
import io.cdap.cdap.metrics.query.MetricsHandler;
import io.cdap.cdap.metrics.query.MetricsQueryService;
import io.cdap.http.HttpHandler;

/**
 * Metrics http handlers.
 */
public class MetricsHandlerModule extends PrivateModule {
  @Override
  protected void configure() {
    bind(MetricsQueryService.class).in(Scopes.SINGLETON);
    expose(MetricsQueryService.class);

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class,
                                                                      Names.named(Constants.Service.METRICS));
    handlerBinder.addBinding().to(MetricsHandler.class);
    CommonHandlers.add(handlerBinder);
  }
}
