/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.metrics.guice;

import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.metrics.process.MetricsProcessorStatusService;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

/**
 * Metrics Processor Service Module for the MetricsProcessorStatusService
 */
public class MetricsProcessorStatusServiceModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(MetricsProcessorStatusService.class).in(Scopes.SINGLETON);
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder
      (binder(), HttpHandler.class, Names.named(Constants.MetricsProcessor.METRICS_PROCESSOR_STATUS_HANDLER));
    handlerBinder.addBinding().to(PingHandler.class);
    expose(MetricsProcessorStatusService.class);
  }
}
